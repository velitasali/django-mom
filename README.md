# Model Object Mapper for Django 

Map YAML files to a database and add/update/delete them as they change.

## Installation

### For Fresh Builds

`pip install -i https://test.pypi.org/simple/ django-mom`

### For Milestone Builds

`pip install django-mom`

## Usage

### Model

```python3
# File: home/models.py

from django.db import models

class Post(models.Model):
    title = models.CharField(max_length=100, )
    date = models.DateTimeField()
    slug = models.SlugField(unique=True, )
```

### Map 

```YAML
# File: mom.yml

mom:
  map:
    post:
      model: home.models.Post
      field: slug
```

### Object

```YAML
# File: post.my-awesome-post.mom.yml
#            ^^^^^^^^^^^^^^^ is `slug` 

field:
  title: My Awesome Title
  date: 2021-06-25 13:00
```

### Result

```YAML
title: My Awesome Title
date: 2021-06-25 13:00
slug: my-awesome-post 
```