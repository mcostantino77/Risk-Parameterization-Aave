import requests
import pandas as pd
import json
from datetime import datetime

# Need to use this index to flatten the dictionary ["data"]["userTransactions"].

query_TotalTransactions = """

query TotalTransactions($txnID: ID) {
  userTransactions(first: 1000, where: {id_gt: $txnID}) {
    id
    user{
        id
    }
  }
}
"""

# Need to use this index to flatten the dictionary ["data"]["liquidationCalls"].

query_TotalLiquidations = """

query TotalLiquidations($txnID: ID) {
  liquidationCalls(first: 1000, where: {id_gt: $txnID}) {
    id
    timestamp
    user{
      id
    }
    collateralReserve{
      name
    }
    collateralAmount
    principalAmount
  }
}
"""

# Need to use this index to flatten the dictionary ["data"]["borrows"].

query_TotalBorrows = """

query TotalBorrows($txnID: ID) {
  borrows(first: 1000, where: {id_gt: $txnID}) {
    id
    timestamp
    user {
      id
    }
    reserve {
      name
    }
    amount
  }
}
"""

# Need to use this index to flatten the dictionary ["data"]["deposits"].

query_TotalDeposits = """

query TotalDeposits($txnID: ID) {
  deposits(first: 1000, where: {id_gt: $txnID}) {
    id
    timestamp
    user {
      id
    }
    reserve {
      name
    }
    amount
  }
}
"""

# Need to use this index to flatten the dictionary ["data"]["repays"].

query_TotalRepays = """

query TotalRepays($txnID: ID) {
  repays(first: 1000, where: {id_gt: $txnID}) {
    id
    timestamp
    user {
      id
    }
    reserve {
      name
    }
    amount
  }
}
"""

query_list = [query_TotalTransactions,                                        # LIST OF ALL QUERIES TO BE RUN
              query_TotalLiquidations, 
              query_TotalBorrows, 
              query_TotalDeposits, 
              query_TotalRepays]

query_entity = ["userTransactions",                                           # LIST OF ALL ENTITIES NEEDED TO QUERY
                "liquidationCalls",
                "borrows",
                "deposits",
                "repays"]

query_dict = {}                                                               # DICTIONARY TO STORE ALL QUERIED DATA
for query in query_list:
    query_dict[query] = pd.DataFrame()

entity = 0
count = 1

for query in query_list:                                                      # NEED "for loop" TO RUN THROUGH EACH QUERY
    
    txnID = {'txnID' : ""}                                                    # NEED TO RESET txnID AFTER EACH LOOP
    time1 = datetime.now()

    while True:
        
        request = requests.post('https://api.thegraph.com/subgraphs/name/aave/protocol-v2', 
                                json={'query': query,                         # MUST UPDATE THE QUERY BEING RUN
                                      'variables': txnID})
        df_test = request.json()
        df_flatten = pd.json_normalize((df_test["data"][query_entity[entity]]))        # MUST UPDATE INDEXING LOGIC BASED ON QUERY
        query_dict[query] = query_dict[query].append(df_flatten, ignore_index = True)  # MUST CREATE NEW DATAFRAME BASED ON QUERY

        txnID_upd = {'txnID' : df_flatten["id"][len(df_flatten)-1]}  
        txnID.update(txnID_upd)

        if len(df_flatten) == 1000:
            count += 1
            continue
        else:
            count += 1
            break

    time2 = datetime.now()
    entity = entity + 1
    print("For query number ", entity, ", it took ", count," iterations and this much time ", time2 - time1, ".", sep = "")
    

# Review the total number of unique pieces 
for query in query_dict:
    print("Query ", list(query_dict.keys()).index(query) + 1, ":", sep = "")
    print(query_dict[query].nunique(), "\n")
