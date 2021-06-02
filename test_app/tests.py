from distutils.dir_util import copy_tree

from django.conf import settings
from django.core.management import call_command
from django.test import TestCase
from tempdir import TempDir

from test_app.models import ContentData, Language, Post, Tag, File


def apply_test_data(folder_name: str):
    with TempDir() as mom_data:
        copy_tree('test_data/%s' % folder_name, mom_data)
        call_command('mom', '-d', mom_data)


class CreateObjects(TestCase):
    def test_populate_simple(self):
        apply_test_data('populate_simple')

        self.assertEquals(Language.objects.count(), 1)

        language = Language.objects.get()

        self.assertEquals('en', language.code)
        self.assertEquals('English', language.name)

        apply_test_data('populate_simple_update')

        language = Language.objects.get()

        self.assertEquals('en', language.code)
        self.assertEquals('İngilizce', language.name)

    def test_populate_all(self):
        def map_tag_slug(tag: Tag):
            return tag.slug

        apply_test_data('populate_all')

        self.assertEquals(ContentData.objects.count(), 2)

        post_one: Post = Post.objects.filter(slug='my-post').get()
        self.assertIsNotNone(post_one.change, 'Change should be not `None`')

        tag_slugs = map(map_tag_slug, post_one.tag.all())
        self.assertIn('pastiche', tag_slugs)
        self.assertIn('sci-fi', tag_slugs)

        post_two: Post = Post.objects.filter(slug='my-second-post').get()
        self.assertEquals(post_two.tag.count(), 0)
        self.assertEquals(post_two.author.profile.first_name, 'Auther')
        self.assertEquals(post_two.author.profile.last_name, 'Writt')

        apply_test_data('populate_all_update')

        post_one.refresh_from_db()
        self.assertIsNone(post_one.change, 'Change should be `None` after update')

        post_two.refresh_from_db()
        self.assertEquals(post_two.author.profile.first_name, 'Auther')
        self.assertEquals(post_two.author.profile.last_name, 'Written')

    def test_feature_implicit_passing(self):
        apply_test_data('feature_implicit_passing')

        post = Post.objects.get()
        only_content = post.content_data.all()[0]
        self.assertEquals(post.author.username, 'tasali')
        self.assertEquals(only_content.language.code, 'en')
        self.assertEquals(only_content.language.name, 'English')

    def test_nested_discovery(self):
        apply_test_data('nested')

        languages = Language.objects.all()
        self.assertEquals(len(languages), 3)

        language_de = Language.objects.filter(code='de').get()
        self.assertEquals(language_de.name, 'Deutsch')

        language_en = Language.objects.filter(code='en').get()
        self.assertEquals(language_en.name, 'English')

        language_tr = Language.objects.filter(code='tr').get()
        self.assertEquals(language_tr.name, 'Türkçe')

    def test_ownership(self):
        apply_test_data('ownership')

        post = Post.objects.get()
        en_content = post.content_data.filter(language__code='en').get()

        self.assertEquals(post.tag.count(), 3)
        self.assertEquals(post.content_data.count(), 2)

        apply_test_data('ownership_update')

        post.refresh_from_db()
        en_content_updated = post.content_data.filter(language__code='en').get()

        self.assertEquals(en_content_updated.content, '#Shorts #MyTube #Update', 'Updated content should match')
        self.assertEquals(en_content.pk, en_content_updated.pk, 'Primary keys should match if updated')
        self.assertEquals(post.tag.count(), 3, 'There should only be 3 tags assigned to this post')
        self.assertEquals(Post.objects.count(), 1, 'Single ownership fields should be deleted when removed')
        self.assertEquals(Tag.objects.count(), 4, 'Shared ownership fields should retain their existence when removed')

    def test_options(self):
        with TempDir() as media_root:
            settings.MEDIA_ROOT = media_root

            apply_test_data('options')

            only_lang = Language.objects.get()
            self.assertEquals(only_lang.name, 'English (US)', 'The name gathered from file should exactly be the same')

            only_file: File = File.objects.get()
            path_first = only_file.file.path

            # Reapply to see if the path changes.
            apply_test_data('options')
            only_file_refreshed: File = File.objects.get()
            path_second = only_file_refreshed.file.path

            self.assertEquals(only_file, only_file_refreshed, 'File should be the same')
            self.assertEquals(path_first, path_second, 'Path should not change, indicating comparison works')

            apply_test_data('options_update')
            only_lang.refresh_from_db()
            only_file_refreshed.refresh_from_db()

            path_third = only_file_refreshed.file.path

            self.assertEquals(only_lang.name, 'English (GB)', 'It should be the same the file content')
            self.assertNotEquals(path_second, path_third, 'File should have changed')
