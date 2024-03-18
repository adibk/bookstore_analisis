from utils import colors as clrs
from utils.colors import clr
from utils.colors import Color
from utils import tools as tls
from utils.tools import N
from utils.debug import TEST

import pandas as pd
import matplotlib.pyplot as plt
import re
from datetime import datetime
import numpy as np

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
    def __init__(self, df, key=None, id=None, clr=Color.RED, length=80):
        self.key = key
        self.df = df
        self.id = id
        self.clr = clr
        self.length = length
        
        self.col = self._count_col()
        self.row = self._count_row()
        self.df_unique = self._get_df_unique()
        self.isunique = self._get_isunique()
        
        self.sep = '-'
        self.sep_clr = Color.BRIGHT_BLACK
    
    def set_clr(self, clr):
        self.clr = clr
    
    def set_len(self, length):
        self.length = length
    
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
    
    # Drop duplicate values and return dataframe
    def _get_df_unique(self):
        if self.id == None:
            return self.df
        return self.df.drop_duplicates(subset=self.id)
    
    # True or False, if df is unique
    def _get_isunique(self):
        if self.df.shape == self.df_unique.shape:
            return True
        return False

    # Print df is unique OR not unique
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
    
    # print shape of a df
    def shape(self, end=''):
        print(self.key, self.df.shape, end, sep='')
            
    def __print_header(self, header=None, length=None, sep=None):
        header = header if header != None else self.key
        header = header if header != None else ''
        length = length if length != None else self.length
        sep = sep if sep != None else self.sep
        r = len(header) % 2
        n = int((length - r - len(header) - 2) / 2)
        print(clr(sep * n, self.sep_clr), clr(header, self.clr), clr(sep * (n + r), self.sep_clr))
    
    def print_sep(self, length=None, sep=None):
        length = length if length != None else self.length
        sep = sep if sep != None else self.sep
        print(clr(sep * length, self.sep_clr), '\n', sep='')
    
    def info(self):
        self.__print_header(self.key + ' info')
        self.df.info()
        self.print_sep()
        
    # print df
    def print(self, length=None, header=None, sep=None):
        self.__print_header(header, length, sep)
        print(self.df)
        self.print_sep(length, sep)
    
    def head(self, n=5):
        return DfHandler(self.df.head(n=n), self.key, self.id, self.clr, self.length)
    
    def tail(self, n=5):
        return DfHandler(self.df.tail(n=n), self.key, self.id, self.clr, self.length)
    
    def print_cols(self):
        for col_name in self.df.columns:
            print(col_name)
        print()
    
    def get_cols(self):
        return self.df.columns
    
    def types(self):
        print(self.df.dtypes, '\n')
    
    def sort(self, col=None):
        if col == None:
            col = self.id
        if col == None:
            return None
        df = self.df.sort_values(by=col, ascending=False)
        return DfHandler(df, self.key, self.id)
    
    def not_match(self, col, pattern):
        invalid_df = self.df[~self.df[col].str.match(pattern)]
        return DfHandler(invalid_df, self.key, self.id)
    
    def match(self, col, pattern):
        df = self.df[self.df[col].str.match(pattern)]
        return DfHandler(df, self.key, self.id)
        
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
    
    def filter(self, col, fct):
        df = self.df[self.df[col].apply(fct)]
        return DfHandler(df, self.key, self.id)
        
    def filter_out(self, col, fct):
        df = self.df[~self.df[col].apply(fct)]
        return DfHandler(df, self.key, self.id)

    def startwith(self, col, substr):
        df = self.df[self.df[col].str.startswith(substr)]
        return self.__new_df(df)
    
    def count(self, col, **kwargs):
        count = self.df[col].value_counts()
        if kwargs.get('put') == True:
            print(count, '\n', sep='')
        return count

    def to(self, col, type):
        if type == 'date':
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
            
    def is_(self, col, col_type):
        pass

    def _to_list(self, *cols):
        combined_list = []
        for col in cols:
            if isinstance(col, list):
                combined_list.extend(col)
            else:
                combined_list.append(col)
        return combined_list
    
    def only(self, *cols):
        cols = self._to_list(*cols)
        return DfHandler(self.df[cols], self.key, self.id, self.clr, self.length)
    
    # use the describe method of panda, pass put=False for no printing
    def describe(self, **args):
        put_arg = args.pop('put', None)
        if args == ():
            df = self.df.describe(include='all')
        else:
            df = self.df.describe(**args)
        df_handler = DfHandler(df, self.key, None, self.clr, self.length)
        if not put_arg or (put_arg and put_arg != False):
            df_handler.print(None, f'{df_handler.key} description')
        return df_handler
        

def sex_distrib_plt(df_handler):
    plt.style.use('seaborn-colorblind')

    colors = ['skyblue', 'lightpink']
    explode = (0.05, 0)

    df_handler.count('sex').plot(kind='pie', 
                                autopct='%1.1f%%', 
                                labels=None, 
                                colors=colors,
                                explode=explode,
                                startangle=90)

    plt.title('Sex Distribution', fontsize=16, fontweight='bold', color='Grey')
    plt.ylabel('')
    plt.legend(labels=['Male', 'Female'], loc='best')
    plt.show()

def age_distrib_plt(df_handler):
    df = df_handler.df
        # Plotting the histogram of ages
    # Calculate bins as one bin per year
    min_age = df['age'].min()
    max_age = df['age'].max()
    bins = range(min_age, max_age + 2)  # +2 to include the last age in the range

    plt.figure(figsize=(10, 6))
    plt.hist(df['age'], bins=bins, color='skyblue', edgecolor='white')
    plt.title('Distribution of Ages')
    plt.xlabel('Age')
    plt.ylabel('Count')

    # Rotate x-axis labels to vertical
    plt.xticks(range(min_age, max_age + 1), rotation=70)

    plt.show()

def get_df_handler(csvs, key, *args):
    return DfHandler(csvs.dfs[key], key, *args)

def handle_dfs(csvs):
    # # Csvs quick visualization just to make sure we using get_df_handler correctly afterward
    # csvs_visu = CsvPrint(csvs)
    # csvs_visu.print_file_paths()
    # csvs_visu.print_all()
    # csvs_visu.print_data('customers')
    
    customers = get_df_handler(csvs, 'customers', 'client_id', Color.BLUE)
    products = get_df_handler(csvs, 'products', 'id_prod', Color.RED)
    transactions = get_df_handler(csvs, 'transactions', None, Color.BRIGHT_GREEN)
    
    # customers.print()
    # products.print()
    # transactions.print()
    
    # customers.info()
    # products.info()
    # transactions.info()

    # customers.head(10).print()
    # customers.tail(10).print()
    # products.head(10).print()
    # products.tail(10).print()
    # transactions.head(10).print()
    # transactions.tail(10).print()
    
    # products.describe()
    # customers.describe()
    # transactions.describe()
    
 
    customers.df['age'] = datetime.now().year - customers.df['birth']
    customers.info()
    customers.describe()
    customers.print()
    
    # Check if sex is f or m only, OK
    customers.not_match('sex', r'^[fm]$').print()
    # check if client_id is c_NUMBER
    # Noticing 2 problematic values ct_0 and ct_1 
    customers.not_match('client_id', r'^c_\d+$').print()

    # sex_distrib_plt(customers)
    # # Noticing a way overrepresantated value in the age distribution, age 20
    # # might be the majority default value when registering, check later with sales date and data collect beginning to see if it is coherent 
    # age_distrib_plt(customers)

    # printing the line where age == 20
    TEST()
    only_20 = customers.filter('age', lambda x: x == 20)
    # Apparently no problem with client_id or sex, so only age must be readjust:
    only_20.print()
    # # Except that 
    # sex_distrib_plt(customers)
    customers.df['client_id_int'] = customers.df['client_id'].str.extract('(\d+)').astype(int)


    customers.print()
    customers.describe()
    # Analyse c_Number, only 2 duplicate client_id_int=1
    duplicates = customers.df[customers.df.duplicated('client_id_int', keep=False)]
    print("Duplicate values in 'client_id_int':")
    print(duplicates)   
    N()
    
    mean_value = customers.df['age'].mean()
    print("Mean of 'age' column:", mean_value)
    # Calculate the median of the 'numbers' column
    median_value = customers.df['age'].median()
    print("Median of 'age' column:", median_value)
    N()
    
    # calcul frequency in order to readjust the age value 20
    # Count the occurrences of each age 
    age_distribution = customers.df['age'].value_counts().reset_index()
    age_distribution.columns = ['age', 'frequency']
    age_distribution = age_distribution.sort_values(by='age').reset_index(drop=True)
    print(age_distribution)
    print(age_distribution.describe())
    # median frequency 136
    # mean 113
    # value directly above (age=21,22,23,24): 146, 146, 129, 136
    df = only_20.df.copy()
    df.loc[:, 'age'] = np.nan
    print(df)
    
    # debug only
    np.random.seed(42)
    # Randomly select 146 rows and set their 'Age' to 20
    rows_to_update = df.sample(n=146, random_state=42).index
    df.loc[rows_to_update, 'age'] = 20
    print(df)
    
    df.print()
    # transactions.print_cols()
    # transactions.only('id_prod').print()

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
    # customers.not_match('sex', r'^[fd]$').print()
    # customers.not_match('client_id', r'^c_\d+$').print()
    # N()
    
    # # Exploring invalid data in products
    # products.is_unique()
    # products.print_cols()
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
    
    # transactions.not_match('id_prod', r'^[012]_\d+$').print()
    # transactions.not_match('client_id', r'^c_\d+$').print()
    # transactions.not_match('session_id', r'^s_[1-9]\d*$').print()
    # transactions.not_match('date', r'^20[012][0-9]-[01][0-9]-[0123][0-9] [012][0-9]:[0-5][0-9]:[0-5][0-9].\d{6}$').print()
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
    
    
    # products.print()
    # customers.print()
    # transactions.print()
    
    
    # # # Step 1: Merge transactions and products
    # transactions_products = pd.merge(transactions.df, products.df, on='id_prod', how='inner')
    # # print(transactions_products.shape)
    
    # # Step 2: Merge the result with customers
    # final_df = pd.merge(transactions_products, customers.df, on='client_id', how='inner')
    
    # # print(final_df.shape)
    # # print(final_df)
    
    # ca = final_df['price'].sum()
    # # print("Chiffre d'affaire:", ca)
    
    # # Book not sold
    # merged_df = pd.merge(products.df, transactions.df, on='id_prod', how='left', indicator=True)
    # not_sold_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['price', 'categ', 'date', 'session_id', 'client_id', '_merge'])
    # # print(not_sold_df)
    # # print(not_sold_df.shape)
    
    # # Customers who havn't made any transactions
    # merged_df = pd.merge(customers.df, transactions.df, on='client_id', how='left', indicator=True)
    # not_customers = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['date', 'session_id', 'sex', 'birth', 'id_prod', '_merge'])
    # # print(not_customers)
    # # print(not_customers.shape)
    
    
    # # Group by 'client_id' and sum 'price', reset index to make 'client_id' a column again
    # aggregated_df = final_df.groupby('client_id', as_index=False)['price'].sum()
    # aggregated_df = final_df.groupby('client_id').agg({
    # 'id_prod': 'last', 
    # 'sex': 'first',
    # 'birth': 'first',
    # 'price': 'sum'    
    # }).reset_index()

    
    # print(aggregated_df.sort_values(by='price', ascending=True))
    # N()
    
    # df_filtered = transactions.df[transactions.df['id_prod'].str.contains('0_2245')]
    # print(df_filtered)
    
    # transactions.print()
    # merged_df = pd.merge(products.df, df_filtered, on='id_prod', how='inner', indicator=True)
    # print(merged_df)
    # merged_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['price', 'categ', 'date', 'session_id', 'client_id', '_merge'])