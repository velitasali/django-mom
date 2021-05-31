import enum
import logging
import os
import re
from os import listdir
from os.path import getsize, getmtime, isdir, isfile, join
from pathlib import Path
from typing import BinaryIO

import yaml
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import QuerySet, Model
from django.utils.module_loading import import_string

from ...settings import MOM_FOLDER, MOM_FILE

KEY_PATTERN = "([a-zA-Z0-9-_]+)"
FILE_PATTERN_FULL = f"^{KEY_PATTERN}.{KEY_PATTERN}.{MOM_FILE}$"
FILE_PATTERN_FOLDER = f"^{KEY_PATTERN}.{MOM_FILE}$"
FILE_PATTERN_PRIVATE = f"^{KEY_PATTERN}$"

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


class DjangoFile:
    file_name: str
    file_size: int
    file: BinaryIO
    date_modified: float

    def __init__(self, file_name: str, file_size: int, file: BinaryIO, date_modified: float) -> None:
        super().__init__()
        self.file_name = file_name
        self.file_size = file_size
        self.file = file
        self.date_modified = date_modified


class Ownership(enum.Enum):
    NONE = 'none'
    SINGLE = 'single'
    SHARED = 'shared'


class MOM:
    mapping: dict
    remapping: dict
    django_models: dict
    implicit_lookup_fields: dict

    def __init__(self, mapping: dict, remapping: dict, django_models: dict, implicit_lookup_fields: dict) -> None:
        super().__init__()
        self.mapping = mapping
        self.remapping = remapping if remapping is not None else {}
        self.django_models = django_models
        self.implicit_lookup_fields = implicit_lookup_fields

    @staticmethod
    def load_from(mom_file: str):
        try:
            with open(mom_file, 'r') as stream:
                yaml_index = yaml.safe_load(stream)
                mom_index = yaml_index['mom']
                mapping = mom_index['map']
                remapping = None if "remap" not in mom_index else mom_index['remap']
                django_models = {}
                implicit_lookup_fields = {}

                unresolvable_models = []
                for map_name, target in mapping.items():
                    if map_name in django_models:
                        raise DuplicateItemException(map_name)

                    lookup_field = target['lookupField']
                    model_import_name = target['model']
                    django_models[map_name] = MOM._map_django_models(model_import_name, map_name)

                    if model_import_name not in unresolvable_models:
                        if model_import_name not in implicit_lookup_fields:
                            implicit_lookup_fields[model_import_name] = lookup_field
                        elif implicit_lookup_fields[model_import_name] != lookup_field:
                            del implicit_lookup_fields[model_import_name]
                            unresolvable_models.append(model_import_name)

                if remapping is not None:
                    for model_import_name, target in remapping.items():
                        if model_import_name in django_models:
                            continue

                        django_models[model_import_name] = MOM._map_django_models(model_import_name, "_REMAP")

                return MOM(mapping, remapping, django_models, implicit_lookup_fields)
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
                matches = re.findall(FILE_PATTERN_FULL, main_file)
                if len(matches) < 1:
                    continue

                (model, map_name) = matches[0]

                if model in mom.mapping:
                    mappers.append(Mapper.load_from(mom, model, map_name, mom_folder, main_file))
            elif isdir(main_full_path):
                model = main_file
                if len(re.findall(FILE_PATTERN_PRIVATE, main_file)) != 1 or model not in mom.mapping:
                    continue

                for child_file in listdir(main_full_path):
                    child_full_path = join(main_full_path, child_file)

                    if isdir(child_full_path):
                        map_name = child_file
                        if len(re.findall(FILE_PATTERN_PRIVATE, map_name)) == 1:
                            mappers.append(Mapper.load_from(mom, model, map_name, child_full_path, mom_file))
                    elif isfile(child_full_path):
                        matches = re.findall(FILE_PATTERN_FOLDER, child_file)
                        if len(matches) < 1:
                            continue

                        (map_name) = matches[0]
                        mappers.append(Mapper.load_from(mom, model, map_name, main_full_path, child_file))

        return mappers

    @property
    def lookup_field_name(self):
        return self.mom.mapping[self.map_name]['lookupField']

    @property
    def model_class(self):
        return self.mom.django_models[self.map_name]

    def _start_mapping(
            self,
            model_class,
            lookup_fields: dict,
            fields: dict,
            ownership: Ownership = Ownership.NONE,
            db_object: Model = None
    ) -> (bool, bool, object):
        lookup_fields = Mapper.flatten_lookup_fields(lookup_fields)
        fields = self.streamline_fields(fields)

        if db_object is None or ownership == Ownership.NONE:
            query: QuerySet = model_class.objects.filter(**lookup_fields)
            result_count = len(query.all())

            if result_count > 1:
                self.logger.error(f"Not unique. There are more than one results for `{self.lookup_field_value}`")
                raise NonUniqueFieldException

            db_object: Model = query.get() if result_count == 1 else None
            updating = db_object is not None
        else:
            updating = True

        if ownership == Ownership.NONE:
            if updating:
                self.logger.debug(f"Skip, non-updatable object `{lookup_fields}`")
                return True, False, db_object
            else:
                self.logger.debug(f"Skip, non-creatable object `{lookup_fields}`")
                return False, False, None

        field_diff = {}

        if updating:
            self.logger.debug(f"Object exists `{lookup_fields}`")
        else:
            self.logger.debug(f"Creation needed for object `{lookup_fields}`")

        for field_name, field_values in fields.items():
            related_model_class = model_class._meta.get_field(field_name).related_model
            if related_model_class is None or field_values is None:
                if updating and isinstance(field_values, DjangoFile):
                    same = False
                    file_field = getattr(db_object, field_name)

                    if file_field is None:
                        self.logger.debug(f"""The file for the field `{field_name}` wasn't defined yet: """
                                          f"""`{lookup_fields}`""")
                    else:
                        try:
                            modified = getmtime(file_field.path)
                            if field_values.file_size == file_field.size and field_values.date_modified == modified:
                                same = True
                            else:
                                self.logger.debug(f"""The file for the field `{field_name}` is different and will """
                                                  f"""be updated for `{file_field.path}`""")
                        except FileNotFoundError:
                            self.logger.debug(f"""The file for the field `{field_name}` doesn't exist: """
                                              f"""`{file_field.path}`""")

                    if not same:
                        self.logger.debug(f"The file for the field `{field_name}` will be updated.")
                        field_diff[field_name] = field_values
                elif not updating or field_values != getattr(db_object, field_name):
                    field_diff[field_name] = field_values
            else:
                remapper = Remapper.create_from(self.mom, related_model_class)
                if isinstance(field_values, list):
                    list_of_fields = []
                    set_m2m = list(getattr(db_object, field_name).all()) if updating else None
                    even = updating and len(set_m2m) == len(field_values)
                    should_update = False

                    for child_field_values in field_values:
                        remapper_local, child_field_values = Remapper.prepare(
                            self, remapper, related_model_class, field_name, child_field_values)
                        child_lookup_fields = Mapper.flatten_lookup_fields(
                            remapper_local.filter_lookup_fields(child_field_values))
                        child_result, child_changed, child_new_value = self._start_mapping(
                            related_model_class, child_lookup_fields, child_field_values, remapper_local.ownership, )

                        if not child_result:
                            self.logger.warning(f"Skip, related field `{field_name}` not ready for `{lookup_fields}`")
                            return False, False, None

                        list_of_fields.append(child_new_value)

                        if not even or child_changed or child_new_value not in set_m2m:
                            should_update = True

                    if len(list_of_fields) > 0 and should_update:
                        if remapper is not None and remapper.ownership == Ownership.SINGLE and set_m2m is not None:
                            for existing_value in set_m2m:
                                existing_value.refresh_from_db()
                                if existing_value not in list_of_fields:
                                    self.logger.debug(f"""Deleting a removed related field from `{field_name}` """
                                                      f"""for `{lookup_fields}`""")
                                    existing_value.delete()

                        field_diff[field_name] = list_of_fields
                else:
                    remapper, field_values = Remapper.prepare(self, remapper, related_model_class, field_name,
                                                              field_values)
                    child_lookup_fields = Mapper.flatten_lookup_fields(remapper.filter_lookup_fields(field_values))
                    child_result, child_changed, child_new_value = self._start_mapping(
                        related_model_class, child_lookup_fields, field_values, remapper.ownership,
                        getattr(db_object, field_name) if updating else None)

                    if not child_result:
                        self.logger.warning(f"Skip, related field `{field_name}` not ready for `{lookup_fields}`")
                        return False, False, None
                    elif not updating or child_changed or child_new_value != getattr(db_object, field_name):
                        field_diff[field_name] = child_new_value

        if len(field_diff) > 0:
            self.logger.debug(f"Saving object `{lookup_fields}`")

            primary_fields = {}
            secondary_fields = {}

            for key, value in field_diff.items():
                if isinstance(value, list) or isinstance(value, DjangoFile):
                    secondary_fields[key] = value
                else:
                    primary_fields[key] = value

            if updating and ownership != Ownership.SHARED:
                for key, value in primary_fields.items():
                    setattr(db_object, key, value)
            else:
                db_object = model_class.objects.create(**primary_fields)

            for key, value in secondary_fields.items():
                if isinstance(value, DjangoFile):
                    file_field = getattr(db_object, key)
                    file_field.save(value.file_name, value.file)
                    os.utime(file_field.path, (value.date_modified, value.date_modified))
                elif isinstance(value, list):
                    getattr(db_object, key).set(value)

            db_object.save()
            self.logger.debug(f"""Object has been {"updated" if updating else "created"} `{lookup_fields}` with """
                              f"""`{list(field_diff.keys())}`""")
        else:
            self.logger.debug(f"Object is up-to-date `{lookup_fields}`")

        return True, len(field_diff) > 0, db_object

    def start_mapping(self) -> bool:
        if not self.loaded:
            self.loaded, _, _ = self._start_mapping(
                self.model_class,
                {self.lookup_field_name: self.lookup_field_value},
                self.fields,
                Ownership.SINGLE,
            )
        return self.loaded

    def streamline_fields(self, fields: dict) -> dict:
        streamlined_fields = {}
        for field_name, field_value in fields.items():
            options = field_name.split(' ')
            if len(options) > 1:
                field_name = options[0]
                options = options[1::]
                if 'file' in options:
                    file_path = join(self.pwd, field_value)
                    try:
                        field_value = Path(file_path).read_text()
                    except Exception as exc:
                        self.logger.error(f"Couldn't read the file '{file_path}' for {field_name}")
                        self.logger.exception(exc)
                        exit(1)
                elif 'djangofile' in options:
                    file_path = join(self.pwd, field_value)
                    try:
                        field_value = DjangoFile(field_value, getsize(file_path), open(file_path, 'rb'),
                                                 getmtime(file_path))
                    except Exception as exc:
                        self.logger.error(f"Couldn't read the file '{file_path}' for {field_name}")
                        self.logger.exception(exc)
                        exit(1)

            streamlined_fields[field_name] = field_value

        return streamlined_fields


class Remapper:
    lookup_fields: list
    ownership: Ownership
    full_class_name: str
    related_model_class: object
    lookup_fields_optional: list = None

    def __init__(
            self,
            lookup_fields: list,
            ownership: Ownership,
            full_class_name: str,
            related_model_class: object,
            lookup_fields_optional: list = None
    ) -> None:
        super().__init__()
        self.lookup_fields = lookup_fields
        self.ownership = ownership
        self.full_class_name = full_class_name
        self.related_model_class = related_model_class
        self.lookup_fields_optional = lookup_fields_optional

    @staticmethod
    def create_from(mom: MOM, related_model_class: object):
        full_class_name = Remapper.full_class_name(related_model_class)
        self_remapping: dict
        lookup_fields: list
        ownership = Ownership.NONE
        lookup_fields_optional: list = []
        logger = logging.getLogger(full_class_name)

        if full_class_name in mom.remapping:
            self_remapping = mom.remapping[full_class_name]

            try:
                lookup_fields = self_remapping['lookupField']
            except KeyError:
                logger.error(f"Missing 'lookupField' field for class <{full_class_name}>")
                raise MissingLookupFieldException

            if 'lookupFieldOptional' in self_remapping:
                lookup_fields_optional = self_remapping['lookupFieldOptional']

            if 'ownership' in self_remapping:
                ownership_value = self_remapping['ownership']
                try:
                    ownership = Ownership(ownership_value)
                except ValueError as exc:
                    logger.error(f"""{ownership_value} is not a possible value for `ownership`. Possible """
                                 f"""values are `{list(map(str, Ownership))}`""")
                    logger.exception(exc)
                    exit(1)
        else:
            return None

        return Remapper(lookup_fields, ownership, full_class_name, related_model_class, lookup_fields_optional)

    @staticmethod
    def prepare(mapper: Mapper, self, related_model_class, field_name, child_fields) -> (object, dict):
        if not isinstance(child_fields, dict):
            if self is not None:
                if len(self.lookup_fields) == 1:
                    child_fields = {self.lookup_fields[0]: child_fields}
                else:
                    logging.error(f"""Implicit passing of field `{field_name}` that holds `{self.full_class_name}` """
                                  f"""is not possible since it doesn't have exactly one lookup field: """
                                  f"""{self.lookup_fields}""")
                    exit(1)
            else:
                full_class_name = Remapper.full_class_name(related_model_class)
                if full_class_name in mapper.mom.implicit_lookup_fields:
                    child_fields = {mapper.mom.implicit_lookup_fields[full_class_name]: child_fields}
                else:
                    logging.error(f"""Field `{field_name}` that holds `{full_class_name}` doesn't have a mapping to """
                                  f"""get the implicit lookup value from. Maybe you didn't intend to pass a value?""")
                    exit(1)

        child_fields = mapper.streamline_fields(child_fields)
        if self is None:
            self = Remapper(list(child_fields.keys()), Ownership.NONE, Remapper.full_class_name(related_model_class),
                            related_model_class)
        return self, child_fields

    def filter_lookup_fields(self, fields: dict):
        lookup_fields: dict = {}
        for lookup_field in self.lookup_fields:
            if lookup_field in fields:
                lookup_fields[lookup_field] = fields[lookup_field]
            else:
                logging.error(f"Lookup field `{lookup_field}` of `{self.full_class_name}` wasn't given for `{fields}`")
                exit(1)

        if self.lookup_fields_optional is not None:
            for lookup_field in self.lookup_fields_optional:
                if lookup_field in fields and lookup_field not in lookup_fields:
                    lookup_fields[lookup_field] = fields[lookup_field]
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
