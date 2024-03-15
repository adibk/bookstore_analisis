import pandas as pd
import re

class CsvHandler:
    def __init__(self, path, file_names):
        self.path = self.make_path(path)
        self.file_names = file_names
        self.file_paths = self.get_file_paths()
        
        self.dfs = {}

    def make_path(self, path):
        foward_slash = ''
        if path[-1] != '/':
            foward_slash = '/'
        return path + foward_slash
    
    def get_file_paths(self):
        file_paths = {}
        for file_name in self.file_names:
            file_paths[file_name] = self.path + file_name + '.csv'
        return file_paths 
                   
    def import_file(self, file_name):
        return pd.read_csv(self.file_paths[file_name])
    
    def init_dfs(self):
        for file in self.file_names:
            self.dfs[file] = self.import_file(file)

class CsvPrint:
    def __init__(self, csvs):
        self.csvs = csvs
        
        print()
        
    def print_file_paths(self):
        for value in self.csvs.file_paths.values():
            print(value)
        print()

    def print_data(self, key):
        print(self.csvs.dfs[key])
        print()
    
    def print_all(self):
        for df in self.csvs.dfs.values():
            print(df)
        print()

class DfHandler:
    def __init__(self, df, key=None, id=None):
        self.key = key
        self.df = df
        self.id = id
        
        self.col = self._count_col()
        self.row = self._count_row()
        self.df_unique = self._get_df_unique()
        self.isunique = self._get_isunique()
        
    def __new_df(self, new_df, key=None, id=None):
        if key == None:
            key = self.key
        if id == None:
            id = self.id
        return DfHandler(new_df, key, id)
        
    def _count_row(self):
        return self.df.shape[0]
    
    def _count_col(self):
        return self.df.shape[1]
    
    # True or False, if df is unique
    def _get_isunique(self):
        if self.df.shape == self.df_unique.shape:
            return True
        return False

    # Print is unique or is not unique
    def is_unique(self, col=None):
        if col == None:
            col = self.id
        neg = ''
        if self.isunique == False:
            neg = 'not '
        print(self.key, 'has ' + neg + 'unique values')
        if col != None:
            print('column used', col)
        print()
    
    # Drop duplicate values and get a clean df
    def _get_df_unique(self):
        if self.id == None:
            return self.df
        return self.df.drop_duplicates(subset=self.id)
    
    # print shape of a df
    def shape(self, end=''):
        print(self.key, self.df.shape, end, sep='')
            
    # print df
    def print(self, border='-', n=30):
        print(border * 30, self.key, border * 30)
        print(self.df)
        print(border * (n * 2 + len(self.key) + 2), '\n', sep='')
        
    def sort(self, col=None):
        if col == None:
            col = self.id
        if col == None:
            return None
        df = self.df.sort_values(by=col, ascending=False)
        return DfHandler(df, self.key, self.id)
    
    def notmatch(self, col, pattern):
        invalid_df = self.df[~self.df[col].str.match(pattern)]
        return DfHandler(invalid_df, self.key, self.id)
    
    def match(self, col, pattern):
        df = self.df[self.df[col].str.match(pattern)]
        return DfHandler(df, self.key, self.id)
    
    def types(self):
        print(self.df.dtypes, '\n')
        
    def max(self, col, display=True):
        max = self.df[col].max()
        if display:
            print(max)
        return max
    
    def min(self, col, display=True):
        min = self.df[col].min()
        if display:
            print(min)
        return min
    
    def print_col(self):
        for col_name in self.df.columns:
            print(col_name)
        print()
            
    def filter(self, col, fct):
        df = self.df[self.df[col].apply(fct)]
        return DfHandler(df, self.key, self.id)
        
    def filter_out(self, col, fct):
        df = self.df[~self.df[col].apply(fct)]
        return DfHandler(df, self.key, self.id)

    def startwith(self, col, substr):
        df = self.df[self.df[col].str.startswith(substr)]
        return self.__new_df(df)
    
    def count(self, col):
        count = self.df[col].value_counts()
        print(count, '\n', sep='')
        return count

    def to(self, col, type):
        if type == 'date':
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
            
    def is_(self, col, col_type):
        pass
    
def get_df_handler(csvs, key, id):
    return DfHandler(csvs.dfs[key], key, id)

def N():
    print()
    
def TEST(s=None):
    if s == None:
        print('--- TEST ---')
    else:
        print(s)

def handle_dfs(csvs):
    # Csvs visualization
    # csvs_visu = CsvPrint(csvs)
    # csvs_visu.print_file_paths()
    # csvs_visu.print_all()
    # csvs_visu.print_data('customers')
    
    customers = get_df_handler(csvs, 'customers', 'client_id')
    products = get_df_handler(csvs, 'products', 'id_prod')
    transactions = get_df_handler(csvs, 'transactions', 'session_id')
    
    # customers.shape()
    # products.shape()
    # transactions.shape('\n')
    
    # # Print all dataframes
    # customers.print()
    # products.print()
    # transactions.print()
    
    # # Exploring invalid data in customers
    # customers.is_unique()
    # customers.max('birth')
    # customers.min('birth')
    # N()
    
    # customers.types()
    # customers.notmatch('sex', r'^[fd]$').print()
    # customers.notmatch('client_id', r'^c_\d+$').print()
    # N()
    
    # # Exploring invalid data in products
    # products.is_unique()
    # products.print_col()
    # products.types()
    # products.max('price')
    # products.min('price')
    # N()
    
    # products.filter('price', lambda x: x <= 0).print()
    # products.filter_out('categ', lambda x: x == 0 or x == 1 or x == 2).print()
    # N()
    
    # # Exploring invalid data in transactions
    # transactions.types()
    # transactions.is_unique()
    # transactions.print()
    
    # transactions.notmatch('id_prod', r'^[012]_\d+$').print()
    # transactions.notmatch('client_id', r'^c_\d+$').print()
    # transactions.notmatch('session_id', r'^s_[1-9]\d*$').print()
    # transactions.notmatch('date', r'^20[012][0-9]-[01][0-9]-[0123][0-9] [012][0-9]:[0-5][0-9]:[0-5][0-9].\d{6}$').print()
    # transactions.sort().print()
    # TEST()
    # print(transactions.startwith('date', 'test').row)
    # N()
    
    # customers.count('sex')
    # customers.count('birth')
    # products.count('categ')
    # products.count('price')
    # transactions.count('session_id')
    # transactions.count('id_prod')
    # transactions.count('client_id')
    
    # transactions.types()
    # transactions.to('date', 'date')
    # transactions.types()
    # transactions.sort().print()
    
    
    products.print()
    customers.print()
    transactions.print()
    
    
    # # Step 1: Merge transactions and products
    transactions_products = pd.merge(transactions.df, products.df, on='id_prod', how='inner')
    # print(transactions_products.shape)
    
    # Step 2: Merge the result with customers
    final_df = pd.merge(transactions_products, customers.df, on='client_id', how='inner')
    
    # print(final_df.shape)
    # print(final_df)
    
    ca = final_df['price'].sum()
    # print("Chiffre d'affaire:", ca)
    
    # Book not sold
    merged_df = pd.merge(products.df, transactions.df, on='id_prod', how='left', indicator=True)
    not_sold_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['price', 'categ', 'date', 'session_id', 'client_id', '_merge'])
    # print(not_sold_df)
    # print(not_sold_df.shape)
    
    # Customers who havn't made any transactions
    merged_df = pd.merge(customers.df, transactions.df, on='client_id', how='left', indicator=True)
    not_customers = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['date', 'session_id', 'sex', 'birth', 'id_prod', '_merge'])
    # print(not_customers)
    # print(not_customers.shape)
    
    
    # Group by 'client_id' and sum 'price', reset index to make 'client_id' a column again
    aggregated_df = final_df.groupby('client_id', as_index=False)['price'].sum()
    aggregated_df = final_df.groupby('client_id').agg({
    'id_prod': 'last', 
    'sex': 'first',
    'birth': 'first',
    'price': 'sum'    
    }).reset_index()

    aggregated_df = aggregated_df[aggregated_df['price'] >= 0]
    print(aggregated_df.sort_values(by='price', ascending=True))
    N()
    
    # df_filtered = transactions.df[transactions.df['id_prod'].str.contains('0_2245')]
    # print(df_filtered)
    
    # transactions.print()
    # merged_df = pd.merge(products.df, df_filtered, on='id_prod', how='inner', indicator=True)
    # print(merged_df)
    # merged_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['price', 'categ', 'date', 'session_id', 'client_id', '_merge'])