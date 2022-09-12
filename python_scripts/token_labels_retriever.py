import pandas as pd
import requests

def table_retriever():
    token_labels = requests.get('https://api.stats.ref.finance/api/last-tvl').json()
    df = pd.json_normalize(token_labels)[['token_account_id', 'ftInfo.name','ftInfo.symbol', 'ftInfo.decimals']]
    df.columns = ['token_contract', 'token', 'symbol', 'decimals']
    return df
    
if __name__ == '__main__':
    df = table_retriever()
    df.to_csv('../data/seeds__token_labels.csv', index=False)