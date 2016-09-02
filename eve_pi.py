from copy import deepcopy
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
        SELECT s.schematicID, p.typeID, s.schematicName, p.quantity, s.cycleTime
        FROM planetSchematics s
        JOIN planetSchematicsTypeMap p ON p.schematicID = s.schematicID AND p.isInput = 0
        '''
    ).fetchall()

    schematics = {
        schematicID: {
            'id': typeID,
            'name': typeName,
            'quantity_per_cycle': quantity,
            'cycle_time': cycleTime,
            'reqs': [],
        } for schematicID, typeID, typeName, quantity, cycleTime in schematics
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
            'quantity': quantity
        })

    schematics = {i['id']: i for i in schematics.values()}

    def drill_down(typeID, required_quantity=None):

        if typeID not in schematics:
            return {'level': 0}

        ret = deepcopy(schematics[typeID])

        if required_quantity is None:
            required_quantity = ret['quantity_per_cycle']
            ret['quantity'] = ret['quantity_per_cycle']
            ret['cycles'] = 1
        else:
            ret['quantity'] = required_quantity
            ret['cycles'] = required_quantity / ret['quantity_per_cycle']

        for i in ret['reqs']:
            i.update(drill_down(i['id'], i['quantity'] * ret['cycles']))

        ret['level'] = max(i['level'] for i in ret['reqs']) + 1

        return ret

    PlanetSchematics.insert([drill_down(i) for i in schematics]).run()


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


# http://stackoverflow.com/a/39301723/2649222
def format_money(f, delimiter=',', frac_digits=2):

    neg = (f < 0)
    if neg:
        f = -f

    s = ('%%.%df' % frac_digits) % f
    if len(s) < 5 + frac_digits:
        return ('-' if neg else '') + s

    l = list(s)
    p = len(s) - frac_digits - 5
    l[p::-3] = [i + delimiter for i in l[p::-3]]

    return ('-' if neg else '') + ''.join(l)


isk = format_money


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
    w.writerow(['id', 'name', 'req_sell_price', 'buy', 'sell'])
    for i in PlanetSchematics.filter(r.expr([3, 4]).contains(r.row['level'])).run():
        w.writerow([
            i['id'],
            i['name'],
            sum(jita_sell[j['name']] * j['input_quantity']
                for j in i['reqs']),
            format_money(jita_buy[i['name']] * i['output_quantity']),
            format_money(jita_sell[i['name']] * i['output_quantity']),
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


def h1(s):
    print('\n\n%s\n%s\n' % (s, '=' * len(s)))


def h2(s):
    print('\n%s\n%s\n' % (s, '-' * len(s)))


def report(d, level):

    assert level >= 0 and level < d['level']

    queue = [('', d)]
    total_sell = 0
    total_buy = 0

    reqs = []

    h1('%s #%d' % (d['name'], d['id']))

    while queue:
        prefix, i = queue.pop(0)
        if i['level'] > level:
            print('%s%s x %d (%d cycles)' % (
                prefix,
                i['name'],
                i['quantity'],
                i['cycles']
            ))
            queue = [(prefix + '  ', j) for j in i['reqs']] + queue
        else:
            sell_cost = get_jita_sell(id=i['id']) * i['quantity']
            total_sell += sell_cost
            total_buy += get_jita_buy(id=i['id']) * i['quantity']
            print('%s%s x %d = %s' % (prefix, i['name'], i['quantity'], isk(sell_cost)))
            reqs.append((i['name'], i['quantity']))

    h2('Summary')

    item_sell = get_jita_sell(id=d['id']) * d['quantity']
    item_buy = get_jita_buy(id=d['id']) * d['quantity']

    print("Requirements sell Jita price: % 16s" % isk(total_sell))
    print("Requirements buy Jita price:  % 16s" % isk(total_buy))
    print("Item sell price in Jita:      % 16s" % isk(item_sell))
    print("Item buy price in Jita:       % 16s" % isk(item_buy))
    print("Sell->Buy profit (immediate): % 16s" % isk(item_buy - total_sell))
    print("Buy->Buy profit:              % 16s" % isk(item_buy - total_buy))
    print("Buy->Sell profit (optimal):   % 16s" % isk(item_sell - total_buy))

    h2('List')

    for name, q in reqs:
        print(name, q)


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


def calculate_all():
    d = [
    ]
    d = [
        (typeID, name, level, int(req_price + 0.5),
         int(get_jita_sell(id=typeID) - req_price + 0.5),
         int(get_jita_buy(id=typeID) - req_price + 0.5))
        for typeID, name, level, req_price in d
    ]
    d = [
        (
            typeID, name, level, req_price,
            jita_sell, jita_buy,
            int(jita_sell / req_price * 100),
            int(jita_buy / req_price * 100),
        )
        for typeID, name, level, req_price, jita_sell, jita_buy in d
    ]
    d.sort(key=lambda x: x[-1])
    return d
