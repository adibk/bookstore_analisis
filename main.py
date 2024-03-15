from clean import clean as cln

def init_main():
    path_data = "data/csv/"
    file_names = ('customers' , 'products' , 'transactions')
    csvs = cln.CsvHandler(path_data, file_names)
    csvs.init_dfs()
    
    return csvs

def exit_main():
    pass

def main():
    csvs = init_main()
    cln.handle_dfs(csvs)
    exit_main()
    
if __name__ == "__main__":
    main()