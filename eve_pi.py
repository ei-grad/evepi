import sys
import csv
import sqlite3

import rethinkdb as r


r.connect().repl()

EVE = r.db('eve')

PlanetSchematics = EVE.table('PlanetSchematics')
MarketOrders = EVE.table('MarketOrders')


def load_schematics():

    conn = sqlite3.connect('/home/ei-grad/Downloads/eve.db')

    schematics = conn.execute(
        '''
        SELECT schematicID, p.typeID, typeName, quantity
        FROM planetSchematicsTypeMap p
        JOIN invtypes t ON t.typeID = p.typeID
        WHERE isInput = 0
        '''
    ).fetchall()

    schematics = {
        i: {
            'id': typeID,
            'name': typeName,
            'output_quantity': quantity,
            'reqs': [],
        } for i, typeID, typeName, quantity in schematics
    }

    reqs = conn.execute(
        '''
        SELECT schematicID, p.typeID, typeName, quantity
        FROM planetSchematicsTypeMap p
        JOIN invtypes t ON t.typeID = p.typeID
        WHERE isInput = 1
        '''
    ).fetchall()

    for i, typeID, typeName, quantity in reqs:
        schematics[i]['reqs'].append({
            'id': typeID,
            'name': typeName,
            'input_quantity': quantity
        })

    PlanetSchematics.insert(schematics.values()).run()


def load_prices(typeIDs):

    def get_query(price_type):
        return r.expr(typeIDs).map(lambda x: r.http(
            'https://crest-tq.eveonline.com/market/10000002/orders/%s/' % price_type,
            params={
                'type': 'https://crest-tq.eveonline.com/inventory/types/' +
                x.coerce_to('STRING') + '/'
            }
        )).concat_map(
            lambda x: r.json(x)['items']
        )

    MarketOrders.filter(lambda x: (
        x['location']['id'] == 60003760
    ) and (
        r.expr(typeIDs).contains(x['type']['id'])
    )).delete().run()

    MarketOrders.insert(get_query('sell')).run()
    MarketOrders.insert(get_query('buy')).run()


def load_pi_prices():
    load_prices(list(set(PlanetSchematics.map(r.row['id']).union(
        PlanetSchematics
        .concat_map(r.row['reqs'])
        .map(r.row['id'])
    ).run())))


def print_report(out=sys.stdout):

    jita_sell = MarketOrders.filter({
        'location': {'id': 60003760},
        'buy': False,
    }).group(
        r.row['type']['name']
    ).map(
        lambda x: x['price']
    ).min().run()

    jita_buy = MarketOrders.filter({
        'location': {'id': 60003760},
        'buy': True,
    }).group(
        r.row['type']['name']
    ).map(
        lambda x: x['price']
    ).max().run()

    w = csv.writer(out)
    w.writerow(['name', 'req_sell_price', 'buy', 'sell'])
    for i in PlanetSchematics.run():
        w.writerow([
            i['name'],
            sum(jita_sell[j['name']] * j['input_quantity']
                for j in i['reqs']),
            jita_buy[i['name']] * i['output_quantity'],
            jita_sell[i['name']] * i['output_quantity'],
        ])


def get_jita_sell(**kwargs):
    return MarketOrders.filter({
        'location': {'id': 60003760},
        'buy': False,
        'type': kwargs,
    }).map(
        lambda x: x['price']
    ).min().run()


def get_jita_buy(**kwargs):
    return MarketOrders.filter({
        'location': {'id': 60003760},
        'buy': True,
        'type': kwargs,
    }).map(
        lambda x: x['price']
    ).max().run()


def drill_down(typeID, level):
    ret = PlanetSchematics.get(typeID).run()
    if ret is None:
        return {}
    if level > 0:
        for i in ret['reqs']:
            i.update(drill_down(i['id'], level - 1))
    return ret


def drill_report(d, required_quantity=1, prefix=''):
    if d.get('reqs'):
        total = 0
        cycles = required_quantity / d['output_quantity']
        print('%s%s x %d (%d cycles)' % (prefix, d['name'], required_quantity,
                                         cycles))
        for i in d['reqs']:
            total += drill_report(i, i['input_quantity'] * cycles, prefix + '  ')
        return total
    else:
        print('%s%s x %d = %.2f' % (
            prefix, d['name'], required_quantity,
            get_jita_sell(id=d['id']) * required_quantity
        ))
        return get_jita_sell(id=d['id']) * required_quantity


def drill_reqs(d, required_quantity=None, ret=None):

    if ret is None:
        ret = {}

    if required_quantity is None:
        required_quantity = d['output_quantity']

    if d.get('reqs'):
        for i in d['reqs']:
            q = i['input_quantity'] * required_quantity / d['output_quantity']
            drill_reqs(i, q, ret)
    else:
        if d['name'] not in ret:
            ret[d['name']] = required_quantity
        else:
            ret[d['name']] += required_quantity

    return ret


def print_reqs(d):
    print('\n'.join('%s %d' % i for i in drill_reqs(d).items()))
