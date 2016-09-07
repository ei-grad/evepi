from copy import deepcopy
import sqlite3

import rethinkdb as r


r.connect().repl()

EVE = r.db('eve')

PlanetSchematics = EVE.table('PlanetSchematics')
MarketOrders = EVE.table('MarketOrders')


def syncdb():

    if 'eve' in r.db_list().run():
        r.db_drop('eve').run()
        r.db_create('eve').run()

    EVE.table_create('PlanetSchematics').run()
    EVE.table_create('MarketOrders').run()

    MarketOrders.index_create('type_location_buy', [
        r.row['type']['id'], r.row['location']['id'], r.row['buy']
    ]).run()


def load_schematics(filename='eve.db'):

    # thanks to Steve Ronuken for https://www.fuzzwork.co.uk/dump/latest/eve.db.bz2
    conn = sqlite3.connect(filename)

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

    PlanetSchematics.delete().run()
    PlanetSchematics.insert([drill_down(i) for i in schematics]).run()


def load_prices(typeIDs):

    if isinstance(typeIDs, list):
        typeIDs = r.expr(typeIDs)

    def get_query(price_type):
        url = 'https://crest-tq.eveonline.com/market/10000002/orders/%s/' % price_type
        return typeIDs.map(lambda x: r.http(
            url,
            params={
                'type': 'https://crest-tq.eveonline.com/inventory/types/' +
                x.coerce_to('STRING') + '/'
            },
            result_format='json',
        )).concat_map(r.row['items'])

    MarketOrders.delete().run()

    MarketOrders.insert(get_query('sell')).run()
    MarketOrders.insert(get_query('buy')).run()


def load_pi_prices():

    load_prices(
        PlanetSchematics['id'].union(
            PlanetSchematics.concat_map(r.row['reqs'])['id']
        ).distinct()
    )

    PlanetSchematics.update(
        lambda x: with_price(x),
        non_atomic=True
    ).run()


def mapreduce_example():
    return MarketOrders.group(
        r.row['type']['name'],
        r.row['location']['name'],
        r.row['buy'],
    ).map(lambda x: (x['buy'], x['price'])).reduce(
        lambda a, b: (a[0], r.branch(
            a[0],
            # for buy prices get max price
            r.max(a[1], b[1]),
            # for sell prices get min price
            r.min(a[1], b[1])
        ))
    ).run()


# http://stackoverflow.com/a/39301723/2649222
def format_money(f, delimiter=',', frac_digits=2):
    '''
    >>> format_money(1.7777)
    '1.78'
    >>> format_money(-1.7777)
    '-1.78'
    >>> format_money(12.7777)
    '12.78'
    >>> format_money(-12.7777)
    '-12.78'
    >>> format_money(123.7777)
    '123.78'
    >>> format_money(-123.7777)
    '-123.78'
    >>> format_money(1234.7777)
    '1,234.78'
    >>> format_money(-1234.7777)
    '-1,234.78'
    >>> format_money(12345.7777)
    '12,345.78'
    >>> format_money(-12345.7777)
    '-12,345.78'
    >>> format_money(123456.7777)
    '123,456.78'
    >>> format_money(-123456.7777)
    '-123,456.78'
    >>> format_money(1234567.7777)
    '1,234,567.78'
    >>> format_money(-1234567.7777)
    '-1,234,567.78'
    >>> format_money(12345678.7777)
    '12,345,678.78'
    >>> format_money(-12345678.7777)
    '-12,345,678.78'
    >>> format_money(123456789.7777)
    '123,456,789.78'
    >>> format_money(-123456789.7777)
    '-123,456,789.78'
    '''

    negative_fix = int(f < 0)

    s = '%.*f' % (frac_digits, f)
    if len(s) < 5 + frac_digits + negative_fix:
        return s

    l = list(s)
    l_fix = l[negative_fix:]
    p = len(l_fix) - frac_digits - 5
    l_fix[p::-3] = [i + delimiter for i in l_fix[p::-3]]

    return ''.join(l[:negative_fix] + l_fix)


isk = format_money


def get_jita_sell(typeID):
    return MarketOrders.get_all(
        [typeID, 60003760, False],
        index='type_location_buy'
    ).map(
        r.row['price']
    ).min().run()


def get_jita_buy(typeID):
    return MarketOrders.get_all(
        [typeID, 60003760, True],
        index='type_location_buy'
    ).map(
        r.row['price']
    ).max().run()


def h1(s):
    print('\n\n%s\n%s\n' % (s, '=' * len(s)))


def h2(s):
    print('\n%s\n%s\n' % (s, '-' * len(s)))


def report(d, level):

    if isinstance(d, int):
        d = with_price(PlanetSchematics.get(d)).run()
    elif isinstance(d, str):
        d = next(with_price(PlanetSchematics.get_all(d, index='name')).run())

    assert level >= 0 and level < d['level']

    queue = [('', d)]
    total_sell = 0
    total_buy = 0

    reqs = []

    h1('%s #%d' % (d['name'], d['id']))

    while queue:
        prefix, i = queue.pop(0)
        if i['level'] > level:
            print('%s%s x %d (%d cycles) %s' % (
                prefix,
                i['name'],
                i['quantity'],
                i['cycles'],
                isk(get_jita_sell(i['id']) * i['quantity'])
            ))
            queue = [(prefix + '  ', j) for j in i['reqs']] + queue
        else:
            sell_cost = i['sell_price'] * i['quantity']
            total_sell += sell_cost
            total_buy += i['buy_price'] * i['quantity']
            print('%s%s x %d = %s' % (prefix, i['name'], i['quantity'], isk(sell_cost)))
            reqs.append((i['name'], i['quantity']))

    h2('Summary')

    item_sell = get_jita_sell(d['id']) * d['quantity']
    item_buy = get_jita_buy(d['id']) * d['quantity']

    print("Requirements sell Jita price:  % 16s" % isk(total_sell))
    print("Requirements buy Jita price:   % 16s" % isk(total_buy))
    print("Item sell price in Jita:       % 16s" % isk(item_sell))
    print("Item buy price in Jita:        % 16s" % isk(item_buy))
    print("Buy->Buy profit (buy diff):    % 16s" % isk(item_buy - total_buy))
    print("Buy->Sell profit (optimal):    % 16s" % isk(item_sell - total_buy))
    print("Sell->Buy profit (immediate):  % 16s" % isk(item_buy - total_sell))
    print("Sell->Sell profit (sell diff): % 16s" % isk(item_sell - total_sell))

    if d['level'] == 4 and level == 1:
        h2('Replace P1 by P3')
        l = []
        for i in d['reqs']:
            if i['level'] == 3:
                s = summary(i, 1)
                l.append('%-35s % 11s' % (
                    '%s %d:' % (i['name'], i['quantity']),
                    '+' + isk(s['item_sell'] - s['reqs_sell']),
                ))
                l.append(
                    '\n'.join(
                        k['name'] + ' ' + str(k['quantity'])
                        for j in i['reqs'] for k in j['reqs']
                    )
                )
        print('\n\n'.join(l))

    h2('List')

    for name, q in reqs:
        print(name, q)


def summary(d, level):

    queue = [d]
    reqs_sell = 0
    reqs_buy = 0

    while queue:
        i = queue.pop(0)
        if i['level'] > level:
            queue.extend(i['reqs'])
        else:
            reqs_sell += get_jita_sell(i['id']) * i['quantity']
            reqs_buy += get_jita_buy(i['id']) * i['quantity']

    item_sell = get_jita_sell(d['id']) * d['quantity']
    item_buy = get_jita_buy(d['id']) * d['quantity']

    return {
        'id': d['id'],
        'name': d['name'],
        'item_sell': item_sell,
        'item_buy': item_buy,
        'reqs_sell': reqs_sell,
        'reqs_buy': reqs_buy,
        'profit': (item_buy - reqs_sell),
    }


def compare_all():
    s = PlanetSchematics.filter(r.row['level'] == 4).run()
    d = [summary(i, 1) for i in s]
    d.sort(key=lambda x: x['profit'])
    return d


def with_price(items, depth=1):

    if depth > 5:
        return None

    def q(typeID, buy):
        return MarketOrders.get_all(
            [typeID, 60003760, buy],
            index='type_location_buy'
        ).filter(
            lambda x: x['volumeEntered'] > 100
        ).map(
            lambda x: x['price']
        )

    return items.merge(lambda x: {
        'buy_price': q(x['id'], True).max(),
        'sell_price': q(x['id'], False).min(),
    }).merge(lambda x: r.branch(
        x['reqs'].default(False),
        {'reqs': with_price(x['reqs'], depth + 1)},
        {},
    )).merge(lambda x: {
        'price_diff': x['sell_price'] - x['buy_price']
    }).merge(lambda x: {
        'price_diff_percents': (x['price_diff'] / x['sell_price']) * 100
    })
