

def createRessource(params,kenesis):
    import uuid
    data={
        "identifier":params["identifier"],
      "active":params["active"],
      "type":params["type"],
      "name":params["name"],
      "alias":params["alias"],
      "telecom":params["telecom"],
      "address":params["address"],
      "partOf":params["partOf"],
      "contact":params["contact"]
      }
    kenesis.put_record(StreamName = 'resourceCreatedEvent',Data=data,PartitionKey=str(uuid.uuid4()))

    return {
        'statusCode': 201,
        'body': data
    }




def createOrUpdateResource(params,kenesis,patch):
    import uuid
    if(patch):
        response=table.get_item(
        Key={
            'identifier':params['identifier']
        }
    )
        keys=list(response.keys())
        if('Item' not in keys):
            return {
            'statusCode': 404,
            'body': response
        }
    data={
        "identifier":params["identifier"],
      "active":params["active"],
      "type":params["type"],
      "name":params["name"],
      "alias":params["alias"],
      "telecom":params["telecom"],
      "address":params["address"],
      "partOf":params["partOf"],
      "contact":params["contact"]
      }
    kenesis.put_record(StreamName = 'resourceUpdatedEvent',Data=data,PartitionKey=str(uuid.uuid4()))
    return {
        'statusCode': 201,
        'body': data
    }


def deleteResource(params,kenesis):
    import uuid
    data={
        "identifier":params["identifier"],
      }
    kenesis.put_record(StreamName = 'resourceDeletedEvent',Data=data,PartitionKey=str(uuid.uuid4()))
    return {
        'statusCode': 201,
        'body': data
    }

def getResources(table):
    response = table.scan()
    empty=table.item_count==0
    if(empty):
        return {
        'statusCode': 204,
        'body': response
    }
    return {
        'statusCode': 200,
        'body': response
    }

def getResource(identifier,table):
    response=table.get_item(
        Key={
            'identifier':identifier
        }
    )
    keys=list(response.keys())
    if('Item' not in keys):
        return {
        'statusCode': 204,
        'body': response
    }
    else:
        return {
        'statusCode': 200,
        'body': response
    }



def resourceCreatedEventHandler(data,table):
    table.put_item(
        Item=data
    )
    return {
        'statusCode': 200,
        'body': data
    }

def resourceUpdatedEventHandler(data,table):
    table.update_item(  
        Key={
            'identifier':data['identifier']
        },
        UpdateExpression="set active = :a, type = :t, name = :n, alias = :al, telecom = :tel, address = :ad, partOf = :p, contact = :c",
        ExpressionAttributeValues={
            ':a': data['active'],
            ':t': data['type'],
            ':n': data['name'],
            ':al': data['alias'],
            ':tel': data['telecom'],
            ':ad': data['address'],
            ':p': data['partOf'],
            ':c': data['contact']
        }
    )
    return {
        'statusCode': 200,
        'body': data
    }

def resourceDeletedEventHandler(data,table):
    table.delete_item(
        Key={
            'identifier':data['identifier']
        }
    )
    return {
        'statusCode': 200,
        'body': data
    }


