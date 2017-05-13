import pandas as pd


class Albums(object):

    def __init__(self):
        self.df = pd.read_csv('csvs/albumlist.csv', encoding="ISO-8859-1")
        self.df['Decade'] = self.df.apply(
            lambda row: self._add_decade(row), axis=1)
        self.df = self._normalize_genre()

    def genres_by_years(self):
        # this is a little ugly and I need to review, but the very basics of
        # the plot work
        genres = self.df
        genres.index = pd.to_datetime(genres['Year'], format='%Y')
        genres = genres['Normalized Genre'].resample('A').sum().apply(
            pd.value_counts).stack().groupby(level=0,
                                             group_keys=False).nlargest(5).unstack(level=-1).fillna(0)
        del genres[0]
        return genres

    def _add_decade(self, row):
        if row['Year'] > 2010:
            return "2010's"
        elif row['Year'] > 2000:
            return "2000's"
        elif row['Year'] > 1990:
            return "1990's"
        elif row['Year'] > 1980:
            return "1980's"
        elif row['Year'] > 1970:
            return "1970's"
        elif row["Year"] > 1960:
            return "1960's"
        else:
            return "1950's"

    def _normalize_genre(self):
        self.df['Normalized Genre'] = self.df['Genre'].str.replace(' & ', '')
        self.df['Normalized Genre'] = self.df[
            'Normalized Genre'].str.replace(' / ', ',')
        self.df['Normalized Genre'] = self.df[
            'Normalized Genre'].str.replace(', ', ',')
        self.df['Normalized Genre'] = self.df['Normalized Genre'].map(
            lambda x: [i.strip() for i in x.split(',')])
        return self.df


# it might make sense to not be a class

# albums = Albums()
# albums.df.to_csv('etl/csvs/album_frame.csv')
# year = albums.group_by_attribute('Year')
# decade = albums.group_by_attribute('Decade')
# artist = albums.albums_by_artist()
# genre = albums.genres_by_years()
# frames = {'year': year,
#           'decade': decade, 'artist': artist, 'genre': genre}
# for key, value in frames.items():
#     value.to_csv('etl/csvs/{0}.csv'.format(key))
