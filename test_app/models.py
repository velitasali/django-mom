import uuid
from os.path import join

from django.db import models


def upload_with_unique_name(instance, filename):
    filename = str(filename)
    position = filename.rfind('.')
    extension = "%s" % filename[position:] if position != -1 and position < len(filename) else str()

    return join(str(type(instance).__name__).lower(), "%s%s" % (str(uuid.uuid1()), extension))


class Language(models.Model):
    code = models.CharField(primary_key=True, max_length=7)
    name = models.CharField(max_length=30)


class ContentData(models.Model):
    language = models.ForeignKey(Language, on_delete=models.PROTECT)
    title = models.CharField(max_length=50, )
    content = models.TextField()


class Tag(models.Model):
    slug = models.SlugField(primary_key=True, )


class Profile(models.Model):
    first_name = models.CharField(max_length=15)
    last_name = models.CharField(max_length=20)


class Author(models.Model):
    username = models.SlugField(primary_key=True)
    email = models.EmailField()
    profile = models.OneToOneField(Profile, on_delete=models.CASCADE)


class Change(models.Model):
    slug = models.SlugField(primary_key=True, )
    name = models.CharField(max_length=20)


class Post(models.Model):
    slug = models.SlugField(primary_key=True, )
    author = models.ForeignKey(Author, on_delete=models.PROTECT)
    change = models.ForeignKey(Change, null=True, on_delete=models.PROTECT)
    content_data = models.ManyToManyField(ContentData, )
    tag = models.ManyToManyField(Tag, )


class File(models.Model):
    slug = models.SlugField(primary_key=True, )
    file = models.FileField(upload_to=upload_with_unique_name)
