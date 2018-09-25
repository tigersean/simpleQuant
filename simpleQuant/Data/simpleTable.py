"""

SimpleTable: simple wrapper around pytables hdf5
------------------------------------------------------------------------------

Example Usage::

  >>> from simpletable import SimpleTable
  >>> import tables

  # define the table as a subclass of simple table.
  >>> class ATable(SimpleTable):
  ...     x = tables.Float32Col()
  ...     y = tables.Float32Col()
  ...     name = tables.StringCol(16)

  # instantiate with: args: filename, tablename
  >>> tbl = ATable('test_docs.h5', 'atable1')

  # insert as with pytables:
  >>> row = tbl.row
  >>> for i in range(50):
  ...    row['x'], row['y'] = i, i * 10
  ...    row['name'] = "name_%i" % i
  ...    row.append()
  >>> tbl.flush()

  # there is also insert_many() method() with takes an iterable
  # of dicts with keys matching the colunns (x, y, name) in this
  # case.

  # query the data (query() alias of tables' readWhere()
  #>>> tbl.query('(x > 4) & (y < 70)') #doctest: +NORMALIZE_WHITESPACE
  array([('name_5', 5.0, 50.0), ('name_6', 6.0, 60.0)],
        dtype=[('name', '|S16'), ('x', '<f4'), ('y', '<f4')])

"""

import tables
_filter = tables.Filters(complevel=5,complib='blosc', shuffle=True)

class SimpleTable(tables.Table):
    def __init__(self, file_name, group_name='default', mode='a', filters=_filter):

        self.f = tables.open_file(file_name, mode, _filter)
        self.uservars = None

        if group_name is None: group_name = 'default'

        self.parentNode=''
        try:
            self.parentNode = self.f.get_node("/", group_name)
        except:
            self.parentNode = self.f.create_group("/", group_name)

        
        self._c_classId = self.__class__.__name__
    
    def filename(self):
        return self.f.filename

    def _get_description(self):
        # pull the description from the attrs
        for attr_name in dir(self):
            if attr_name[0] == '_': continue
            try:
                attr = getattr(self, attr_name)
            except:
                continue
            if isinstance(attr, tables.Atom):
                yield attr_name, attr


    def create_table(self, table_name,description=None):
        if table_name in self.parentNode: # existing table
            description = None
        elif description is None: # pull the description from the attrs
            description = dict(self._get_description())

        tables.Table.__init__(self, self.parentNode, table_name,
                       description=description,  _log=False)

    def insert_many(self, table_name, data_generator, attr=False):
        self.create_table(table_name)
        row = self.row
        cols = self.colnames
        for i, d in data_generator.iterrows():
            for c in cols:
                row[c] = d[c]
                print(row[c], d[c])
                row.append()
        self.flush()

    query = tables.Table.read_where

# convience sublcass that i use a lot.
class BlastTable(SimpleTable):
      query      = tables.StringCol(5)
      subject    = tables.StringCol(5)

      pctid      = tables.Float32Col()
      hitlen     = tables.UInt16Col()
      nmismatch  = tables.UInt16Col()
      ngaps      = tables.UInt16Col()

      qstart     = tables.UInt32Col()
      qstop      = tables.UInt32Col()
      sstart     = tables.UInt32Col()
      sstop      = tables.UInt32Col()

      evalue     = tables.Float64Col()
      score      = tables.Float32Col()


if __name__ == '__main__':
    pass