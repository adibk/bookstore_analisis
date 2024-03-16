from dfs import dfs as dfs

def init_main():
    path_data = "data/csv/"
    file_names = ('customers' , 'products' , 'transactions')
    csvs = dfs.CsvHandler(path_data, file_names)
    csvs.init_dfs()
    
    return csvs

def exit_main():
    pass

def main():
    csvs = init_main()
    dfs.handle_dfs(csvs)
    exit_main()
    
if __name__ == "__main__":
    main()