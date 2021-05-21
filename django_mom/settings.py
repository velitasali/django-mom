from django.conf import settings

MOM_FOLDER = getattr(settings, 'MOM_FOLDER', 'mom_data')
MOM_FILE = getattr(settings, 'MOM_FILE', 'mom.yml')