import psycopg2
import json
from based import based

def get_home_addr(locations):
    for data in locations:
        if 'type' in data:
            if data['type'] == 'RESIDENCE' or data['type'] == 'R':
                addr = {'street_addr':'', 'city': '', 'county': '', 'state': '', 'zipcode': ''}
                if 'streetAddr' in data:
                    addr['street_addr'] = data['streetAddress']
                if 'city' in data:
                    addr['city'] = data['city']
                if 'county' in data:
                    addr['county'] = data['county']
                if 'state' in data:
                    addr['state'] = data['state']
                if 'zipCode' in data:
                    addr['zipcode'] = data['zipCode']
                return addr
    return None

def clean_offenders(raw_offenders):
    offenders = []
    for o in raw_offenders:
        offender = {}
        offender['first_name'] = o['name']['givenName']
        offender['last_name'] = o['name']['surName']
        offender['age'] = str(o.get('age'))
        offender['home_addr'] = get_home_addr(o['locations'])
        zipcode = None
        if offender['home_addr'] is not None:
            zipcode = offender['home_addr']['zipcode']
        offender['info_link'] = o.get('offenderUri')
        offender['image_link'] = o.get('imageUri')
        offender['state'] = o.get('jurisdictionId')
        offender['zipcode'] = zipcode

        offenders.append(offender)
    return offenders

def insert_offenders(offenders):
    db = based()
    db.connect()
    query = """
    INSERT INTO offenders (first_name, last_name, age, home_addr, info_link, image_link, state, zipcode)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    if offenders != None:
        offender_values =[
            (
                offender["first_name"],
                offender["last_name"],
                offender["age"],
                json.dumps(offender["home_addr"]) if offender['home_addr'] else None,
                offender["info_link"],
                offender["image_link"],
                offender["state"],
                offender["zipcode"]
            )
            for offender in offenders
        ]
        try:
            db.cursor.executemany(query, offender_values)
            db.connection.commit()
        except Exception as e:
            print(f'error: {e}')
            db.connection.rollback()
    db.close()
