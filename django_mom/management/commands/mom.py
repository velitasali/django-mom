import logging
import re
from os import listdir
from os.path import isdir, isfile, join
from pathlib import Path

import yaml
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import QuerySet, Model
from django.utils.module_loading import import_string

from ...settings import MOM_FOLDER, MOM_FILE

KEY_PATTERN = "([a-zA-Z0-9-_]+)"
FULL_PATTERN = f"^{KEY_PATTERN}.{KEY_PATTERN}.{MOM_FILE}$"
FILE_PATTERN = f"^{KEY_PATTERN}$"

logging.basicConfig(level=logging.NOTSET)


class MOMException(Exception):
    pass


class DuplicateItemException(MOMException):
    pass


class NonUniqueFieldException(MOMException):
    pass


class MissingLookupFieldException(MOMException):
    pass


class UnsupportedValueException(MOMException):
    pass


class MOM:
    mapping: dict = None
    remapping: dict = None
    django_models: dict = None

    def __init__(self, mapping, remapping, django_models) -> None:
        super().__init__()
        self.mapping = mapping
        self.remapping = remapping if remapping is not None else {}
        self.django_models = django_models

    @staticmethod
    def load_from(mom_file: str):
        try:
            with open(mom_file, 'r') as stream:
                yaml_index = yaml.safe_load(stream)
                mom_index = yaml_index['mom']
                mapping = mom_index['map']
                remapping = None if "remap" not in mom_index else mom_index['remap']
                django_models = {}

                for map_name, target in mapping.items():
                    if map_name in django_models:
                        raise DuplicateItemException(map_name)

                    django_models[map_name] = MOM._map_django_models(target['model'], map_name)

                if remapping is not None:
                    for model_import_name, target in remapping.items():
                        if model_import_name in django_models:
                            continue

                        django_models[model_import_name] = MOM._map_django_models(model_import_name, "_REMAP")

                return MOM(mapping, remapping, django_models)
        except IOError:
            logging.error(f"Couldn't open '{mom_file}' file.")
            exit(1)
        except Exception as exc:
            raise exc

    @staticmethod
    def _map_django_models(model_import_name: str, map_name: str):
        try:
            logging.info(f"Locating Django model: {model_import_name}")
            return import_string(model_import_name)
        except ImportError:
            logging.error(f"Could not import `{model_import_name} for `{map_name}` defined in {MOM_FILE}")
            exit(1)


class Mapper:
    mom: MOM
    map_name: str
    fields: dict
    mapping: dict
    pwd: str
    file: str
    lookup_field_value: str
    loaded = False
    logger: logging

    def __init__(self, mom: MOM, map_name: str, pwd: str, file: str, fields: dict, lookup_field_value: str) -> None:
        super().__init__()
        self.mom = mom
        self.map_name = map_name
        self.fields = fields
        self.pwd = pwd
        self.file = file
        self.lookup_field_value = lookup_field_value
        self.logger = logging.getLogger("mapper:%s:%s" % (map_name, lookup_field_value))

        if self.lookup_field_name not in self.fields:
            self.fields[self.lookup_field_name] = self.lookup_field_value

    @staticmethod
    def flatten_lookup_fields(lookup_fields: dict, parent_key: str = None, flattened_fields=None) -> dict:
        if flattened_fields is None:
            flattened_fields = {}
        for key, value in lookup_fields.items():
            qual_key = key if parent_key is None else f"{parent_key}__{key}"
            if isinstance(value, dict):
                Mapper.flatten_lookup_fields(value, qual_key, flattened_fields)
            elif isinstance(value, list):
                logging.error("A list value cannot be flattened")
                raise UnsupportedValueException
            else:
                flattened_fields[qual_key] = value

        return flattened_fields

    @staticmethod
    def load_from(mom: MOM, map_name: str, lookup_field_value: str, pwd, file):
        logger = logging.getLogger(map_name)
        mom_file = join(pwd, file)
        fields = None

        try:
            with open(mom_file, 'r') as stream:
                fields = yaml.safe_load(stream)['field']
        except Exception as exc:
            logger.exception(exc)
            exit(1)

        mapper = Mapper(mom, map_name, pwd, file, fields, lookup_field_value)

        logger.info(f"""Loaded object: `{mapper.lookup_field_value}:{mapper.lookup_field_name}` of `{map_name}` """
                    f"""from {join(pwd, file)}""")

        return mapper

    @staticmethod
    def load_mappers_from(mom: MOM, mom_folder: str, mom_file: str) -> list:
        mappers = []

        for main_file in listdir(mom_folder):
            main_full_path = join(mom_folder, main_file)

            if isfile(main_full_path):
                matches = re.findall(FULL_PATTERN, main_file)
                if len(matches) < 1:
                    continue

                (model, map_name) = matches[0]

                if model in mom.mapping:
                    mappers.append(Mapper.load_from(mom, model, map_name, mom_folder, main_file))
            elif isdir(main_full_path):
                model = main_file
                if len(re.findall(FILE_PATTERN, main_file)) != 1 or model not in mom.mapping:
                    continue

                for child_file in listdir(main_full_path):
                    map_name = child_file
                    child_full_path = join(main_full_path, map_name)

                    if len(re.findall(FILE_PATTERN, map_name)) == 1 and isdir(child_full_path):
                        mappers.append(Mapper.load_from(mom, model, map_name, child_full_path, mom_file))

        return mappers

    @property
    def lookup_field_name(self):
        return self.mom.mapping[self.map_name]['field']

    @property
    def model_class(self):
        return self.mom.django_models[self.map_name]

    def _start_mapping(
            self,
            model_class,
            lookup_fields: dict,
            fields: dict,
            creatable: bool = True,
            updatable: bool = True,
            db_object: Model = None
    ) -> (bool, bool, object):
        lookup_fields = Mapper.flatten_lookup_fields(lookup_fields)

        if creatable:
            updatable = True

        if db_object is None or not updatable:
            query: QuerySet = model_class.objects.filter(**lookup_fields)
            result_count = len(query.all())

            if result_count > 1:
                self.logger.error(f"Not unique. There are more than one results for `{self.lookup_field_value}`")
                raise NonUniqueFieldException

            updating = result_count == 1
            db_object: Model = query.get() if updating else None
        else:
            updating = True

        if not creatable and not updating:
            self.logger.debug(f"Skip, non-creatable object `{lookup_fields}`")
            return False, False, None
        elif not updatable and updating:
            self.logger.debug(f"Skip, non-updatable object `{lookup_fields}`")
            return True, False, db_object

        field_diff = {}
        many2many_diff = {}

        if updating:
            self.logger.debug(f"Object exists `{lookup_fields}`")
        else:
            self.logger.debug(f"Creation needed for object `{lookup_fields}`")

        for field_name, field_values in fields.items():
            options = None
            possible_options = field_name.split(' ')
            if len(possible_options) > 1:
                field_name = possible_options[0]
                options = possible_options[1::]

            related_model_class = model_class._meta.get_field(field_name).related_model
            if related_model_class is None:
                if options is not None and "file" in options:
                    file_path = join(self.pwd, field_values)
                    try:
                        field_values = Path(file_path).read_text()
                    except Exception as exc:
                        self.logger.error(f"Couldn't read the file '{file_path}' for {lookup_fields}")
                        self.logger.exception(exc)
                        exit(1)

                if not updating or field_values != getattr(db_object, field_name):
                    field_diff[field_name] = field_values
            else:
                remapper = Remapper.create_from(self.mom, related_model_class)
                if isinstance(field_values, dict):
                    if remapper is None:
                        remapper = Remapper.create_custom_from(related_model_class, field_values)

                    child_lookup_fields = Mapper.flatten_lookup_fields(remapper.filter_lookup_fields(field_values))
                    child_result, child_changed, child_new_value = self._start_mapping(
                        related_model_class, child_lookup_fields, field_values, remapper.creatable,
                        remapper.updatable, getattr(db_object, field_name) if updating else None)

                    if not child_result:
                        self.logger.warning(f"Skip, related field `{field_name}` not ready for `{lookup_fields}`")
                        return False, False, None
                    elif not updating or child_changed or child_new_value != getattr(db_object, field_name):
                        field_diff[field_name] = child_new_value
                elif isinstance(field_values, list):
                    list_of_fields = []
                    set_m2m = list(getattr(db_object, field_name).all()) if updating else None
                    even = updating and len(set_m2m) == len(field_values)
                    should_update = False

                    for child_field_values in field_values:
                        remapper_local = Remapper.create_custom_from(
                            related_model_class, child_field_values
                        ) if remapper is None else remapper

                        child_lookup_fields = Mapper.flatten_lookup_fields(
                            remapper_local.filter_lookup_fields(child_field_values))
                        child_result, child_changed, child_new_value = self._start_mapping(
                            related_model_class, child_lookup_fields, child_field_values, remapper_local.creatable,
                            remapper_local.updatable, )

                        list_of_fields.append(child_new_value)

                        if not child_result:
                            self.logger.warning(f"Skip, related field `{field_name}` not ready for `{lookup_fields}`")
                            return False, False, None
                        elif not even or child_changed or child_new_value not in set_m2m:
                            should_update = True

                    if len(list_of_fields) > 0 and should_update:
                        many2many_diff[field_name] = list_of_fields

        if len(field_diff) > 0 or len(many2many_diff) > 0:
            self.logger.debug(f"Saving object `{lookup_fields}`")

            if updating:
                for key, value in field_diff.items():
                    setattr(db_object, key, value)
            else:
                db_object = model_class.objects.create(**field_diff)

            for key, value in many2many_diff.items():
                getattr(db_object, key).set(value)

            db_object.save()
            self.logger.debug(f"""Object has been {"updated" if updating else "created"} `{lookup_fields}` with """
                              f"""`{field_diff}`""")
        else:
            self.logger.debug(f"Object is up-to-date `{lookup_fields}`")

        return True, len(field_diff) > 0 or len(many2many_diff) > 0, db_object

    def start_mapping(self) -> bool:
        if not self.loaded:
            self.loaded, _, _ = self._start_mapping(
                self.model_class,
                {self.lookup_field_name: self.lookup_field_value},
                self.fields
            )
        return self.loaded


class Remapper:
    from_fields: list
    creatable: bool
    updatable: bool
    full_class_name: str
    related_model_class: object

    def __init__(
            self,
            from_fields: list,
            creatable: bool,
            updatable: bool,
            full_class_name: str,
            related_model_class: object
    ) -> None:
        super().__init__()
        self.from_fields = from_fields
        self.creatable = creatable
        self.updatable = updatable
        self.full_class_name = full_class_name
        self.related_model_class = related_model_class

    @staticmethod
    def create_from(mom: MOM, related_model_class: object):
        full_class_name = Remapper.full_class_name(related_model_class)
        self_remapping: dict
        from_fields: list
        creatable = False
        updatable = False
        logger = logging.getLogger(full_class_name)

        if full_class_name in mom.remapping:
            self_remapping = mom.remapping[full_class_name]

            try:
                from_fields = self_remapping['from']
            except KeyError:
                logger.error(f"Missing 'from' field for class <{full_class_name}>")
                raise MissingLookupFieldException

            if "create" in self_remapping:
                creatable = self_remapping['create']

            if "update" in self_remapping:
                updatable = self_remapping['update']
        else:
            return None

        return Remapper(from_fields, creatable, updatable, full_class_name, related_model_class)

    @staticmethod
    def create_custom_from(related_model_class, fields: dict):
        return Remapper(list(fields.keys()), False, False, Remapper.full_class_name(related_model_class),
                        related_model_class)

    def filter_lookup_fields(self, fields: dict):
        lookup_fields: dict = {}
        for field, value in fields.items():
            if field in self.from_fields:
                lookup_fields[field] = value
        return lookup_fields

    @staticmethod
    def full_class_name(related_model_class: object):
        return "%s.%s" % (related_model_class.__module__, related_model_class.__qualname__)


@transaction.non_atomic_requests
def mom_run():
    logger = logging.getLogger(__name__)
    mom_file = join(MOM_FOLDER, MOM_FILE)
    mom = MOM.load_from(mom_file)
    mappers = Mapper.load_mappers_from(mom, MOM_FOLDER, MOM_FILE)

    while True:
        had_successful = False
        had_missing = False

        for mapper in mappers:
            if mapper.loaded:
                continue
            else:
                had_missing = True

            if mapper.start_mapping():
                had_successful = True

        if had_missing:
            if not had_successful:
                for mapper in mappers:
                    if not mapper.loaded:
                        logger.error(f"""Failed => `{mapper.lookup_field_value}:{mapper.lookup_field_name}` """
                                     f"""of `{mapper.map_name}`""")

                logger.error("Failed to complete.")
                exit(1)
        else:
            break

    logger.info("Successful.")


class Command(BaseCommand):
    def handle(self, *args, **options):
        mom_run()
