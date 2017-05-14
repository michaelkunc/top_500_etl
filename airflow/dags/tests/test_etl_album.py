import unittest
from etl_classes import albums


class Album_Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        Album_Test.data = albums.Albums()

    def test_data_frame_shape(self):
        self.assertEqual((500, 8),
                         Album_Test.data.df.shape)

    def test_column_headers(self):
        self.assertEqual(['Number', 'Year', 'Album', 'Artist',
                          'Genre', 'Subgenre', 'Decade', 'Normalized Genre'], list(Album_Test.data.df.columns))

    def test_first_row(self):
        self.assertEqual([1, 1967, "Sgt. Pepper's Lonely Hearts Club Band", "The Beatles",
                          "Rock", "Rock & Roll, Psychedelic Rock", "1960's", ['Rock']], list(Album_Test.data.df.ix[0]))

    def test_min__max_years(self):
        self.assertEqual((1955, 2011), tuple(
            (int(Album_Test.data.df['Year'].min()), int(Album_Test.data.df['Year'].max()))))

    def test_normalize_genre_ampersand(self):
        self.assertEqual(['Folk', 'World', 'Country'], Album_Test.data.df[
                         'Normalized Genre'].iloc[476])

    def test_normalize_genre_slash(self):
        self.assertEqual(['Hip Hop', 'Funk', 'Soul'], Album_Test.data.df[
                         'Normalized Genre'].iloc[480])

    def test_normalize_genre_extra_space(self):
        self.assertEqual(['Rock', 'Blues'], Album_Test.data.df[
                         'Normalized Genre'].iloc[8])

    def test_genres_by_years(self):
        self.assertEqual(
            (57, 15,), Album_Test.data.genres_by_years().shape)
