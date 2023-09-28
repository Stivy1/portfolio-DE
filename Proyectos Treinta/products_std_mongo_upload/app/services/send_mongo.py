
def send_products_mongo(mongoCollection, requests):

    try:
        #print(_min,_max,len(requests[_min:_max]))
        result = mongoCollection.bulk_write(requests, ordered=False)
        response = result.bulk_api_result
        #print(response)
        if response['writeErrors'] != []:
            print(response['writeErrors'])
    except Exception as bwe:
        print(bwe)