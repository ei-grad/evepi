from copy import deepcopy
import sqlite3
import asyncio
import logging

import aiohttp
from aiohttp.protocol import HttpMessage

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


CREST_URL = 'https://crest-tq.eveonline.com/'


def fetch_orders(types, region_id=10000002, loop=None):
    urls = [
        {'url': CREST_URL + 'market/%d/orders/%s/' % (region_id, order_type),
         'params': {'type': CREST_URL + 'inventory/types/%s/' % type_id}}
        for type_id in types
        for order_type in ['sell', 'buy']
    ]
    return fetch_items(urls)


def fetch_items(kwargs_list, loop=None):

    futures = []

    if loop is None:
        loop = asyncio.get_event_loop()

    with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=20),
        headers={'User-Agent': 'PI Master (https://pi.ei-grad.ru %s)' % (HttpMessage.SERVER_SOFTWARE)}
    ) as session:

        async def get_items(**kwargs):
            async with session.get(**kwargs) as resp:
                logging.info("get_items response: %s %s", kwargs, resp)
                data = await resp.json()
                logging.debug("get_items json: %s %s", kwargs, data)
                if 'next' in data:
                    futures.append(loop.create_task(get_items(url=data['next']['href'])))
                return data['items']

        for i in kwargs_list:
            futures.append(loop.create_task(get_items(**i)))

        while futures:
            f = futures.pop(0)
            loop.run_until_complete(f)
            yield from f.result()


def load_prices_rethink(typeIDs):

    if isinstance(typeIDs, list):
        typeIDs = r.expr(typeIDs)

    def get_query(price_type):
        url = CREST_URL + 'market/10000002/orders/%s/' % price_type
        return typeIDs.map(lambda x: r.http(
            url,
            params={
                'type': CREST_URL + 'inventory/types/' + x.coerce_to('STRING') + '/'
            },
            result_format='json',
        )).concat_map(r.row['items'])

    MarketOrders.insert(get_query('sell'), conflict='replace').run()
    MarketOrders.insert(get_query('buy'), conflict='replace').run()


def load_prices(typeIDs):
    MarketOrders.insert(fetch_orders(typeIDs), conflict='replace').run()


def load_pi_prices():

    ret1 = load_prices(
        PlanetSchematics.map(lambda x: x['id']).union(
            PlanetSchematics.concat_map(r.row['reqs']).map(lambda x: x['id'])
        ).distinct().run()
    )

    ret2 = PlanetSchematics.update(
        lambda x: with_price(x),
        non_atomic=True
    ).run()

    return {
        'prices': ret1,
        'schematics': ret2,
    }


def load_all_prices():
    return load_prices(i['id'] for i in fetch_items([{
        'url': 'https://crest-tq.eveonline.com/market/types/',
    }]))


def mapreduce_example():
    return MarketOrders.filter(r.and_(
        r.row['location']['id'] == 60003760,
        r.row['price'] * r.row['volume'] > 10000000,
    )).group(
        r.row['type']['name'],
    ).map(lambda x: r.branch(
        x['buy'],
        {'buy_price': x['price']},
        {'sell_price': x['price']},
    )).reduce(lambda a, b: {
        'buy_price': r.max([
            a['buy_price'].default(0),
            b['buy_price'].default(0),
        ]),
        'sell_price': r.min([
            a['sell_price'].default(1e100),
            b['sell_price'].default(1e100),
        ]),
    }).ungroup().has_fields({
        'reduction': ['sell_price', 'buy_price']
    }).map(lambda x: x['reduction'].merge({
        'name': x['group'],
        'diff': x['reduction']['sell_price'] - x['reduction']['buy_price'],
    })).order_by('diff').run()


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

    h1('%s #%d' % (d['name'], d['id']))

    queue = [('', d)]
    total_sell = 0
    total_buy = 0

    reqs = []

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

    item_sell = d['sell_price'] * d['quantity']
    item_buy = d['buy_price'] * d['quantity']

    print("Requirements sell Jita price:  % 16s" % isk(total_sell))
    print("Requirements buy Jita price:   % 16s" % isk(total_buy))
    print("Item sell price in Jita:       % 16s" % isk(item_sell))
    print("Item buy price in Jita:        % 16s" % isk(item_buy))
    print("Buy->Buy profit (buy diff):    % 16s" % isk(item_buy - total_buy))
    print("Buy->Sell profit (optimal):    % 16s" % isk(item_sell - total_buy))
    print("Sell->Buy profit (immediate):  % 16s" % isk(item_buy - total_sell))
    print("Sell->Sell profit (sell diff): % 16s" % isk(item_sell - total_sell))

    h2('List')
    for name, q in reqs:
        print(name, q)

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
