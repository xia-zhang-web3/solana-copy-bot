"""Independent raw JSON transaction decoder; integer evidence, never UI string equality."""
import base64
import json
import pathlib
import struct

PUMP = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
WSOL = 'So11111111111111111111111111111111111111112'
SYSTEM = '11111111111111111111111111111111'
TOKEN = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
TOKEN22 = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
IDL = json.loads((pathlib.Path(__file__).resolve().parents[1] / 'private/pump_amm.idl.json').read_text())
SWAPS = {bytes(x['discriminator']): x for x in IDL['instructions'] if x['name'] in ('buy','buy_exact_quote_in','sell')}
EVENTS = {bytes(x['discriminator']): x['name'] for x in IDL['events'] if x['name'] in ('BuyEvent','SellEvent')}
TYPES = {x['name']: x for x in IDL['types']}


def b58decode(value):
    n = 0
    for c in value:
        n = n*58 + ALPHABET.index(c)
    return b'\0'*(len(value)-len(value.lstrip('1'))) + (n.to_bytes((n.bit_length()+7)//8,'big') if n else b'')


def b58encode(data):
    n = int.from_bytes(data,'big')
    out = ''
    while n:
        n, r = divmod(n,58)
        out = ALPHABET[r] + out
    return '1'*(len(data)-len(data.lstrip(b'\0'))) + out


def decode_event(data):
    name = EVENTS.get(data[:8])
    if name is None:
        return None
    fields, offset = {}, 8
    for f in TYPES[name]['type']['fields']:
        typ = f['type']
        size = {'u64':8,'i64':8,'u128':16,'i128':16,'pubkey':32,'bool':1}.get(typ)
        if typ == 'string':
            if offset+4 > len(data): break
            size = 4 + int.from_bytes(data[offset:offset+4],'little')
        if size is None or offset+size > len(data): break
        raw = data[offset:offset+size]
        if typ == 'pubkey': value=b58encode(raw)
        elif typ == 'bool': value=bool(raw[0])
        elif typ == 'string': value=raw[4:].decode()
        else: value=int.from_bytes(raw,'little',signed=typ.startswith('i'))
        fields[f['name']] = value
        offset += size
    return {'type':name, 'fields':fields, 'decoded_bytes':offset, 'total_bytes':len(data)}


def instructions(result):
    msg, meta = result['transaction']['message'], result['meta']
    static = msg['accountKeys']
    if not all(isinstance(k,str) for k in static):
        raise ValueError('json_account_keys_required')
    loaded = meta.get('loadedAddresses')
    if loaded is None and msg.get('addressTableLookups'):
        raise ValueError('missing_loaded_addresses')
    loaded = loaded or {'writable':[], 'readonly':[]}
    keys = static + loaded['writable'] + loaded['readonly']
    if len(keys)!=len(set(keys)): raise ValueError('duplicate_account_key')
    groups = {g['index']: (i,g['instructions']) for i,g in enumerate(meta['innerInstructions'])}
    if len(groups) != len(meta['innerInstructions']):
        raise ValueError('duplicate_inner_group')
    flat = []
    for i, outer in enumerate(msg['instructions']):
        entries = [(outer, f'transaction.message.instructions[{i}]', 1)]
        if i in groups:
            gidx, inner = groups[i]
            entries += [(ix,f'meta.innerInstructions[{gidx}].instructions[{j}]', ix.get('stackHeight'))
                        for j,ix in enumerate(inner)]
        active = []
        for ix,path,depth in entries:
            if depth is None:
                raise ValueError('missing_cpi_stack_height')
            if any(not isinstance(n,int) or n<0 or n>=len(keys) for n in [ix['programIdIndex']]+ix['accounts']):
                raise ValueError('bad_account_index')
            while active and active[-1][0] >= depth: active.pop()
            data = b58decode(ix['data'])
            item = {'path':path,'depth':depth,'program':keys[ix['programIdIndex']],
                    'accounts':[keys[k] for k in ix['accounts']], 'account_indices':ix['accounts'],
                    'data':data,'parent_swap':active[-1][1] if active else None}
            if item['program'] == PUMP and data[:8] in SWAPS:
                spec = SWAPS[data[:8]]
                if len(data)<24 or len(item['accounts'])<19:
                    raise ValueError('incomplete_swap_instruction')
                item['swap'] = spec['name']
                item['named'] = {a['name']: item['accounts'][j] for j,a in enumerate(spec['accounts']) if j<len(item['accounts'])}
                item['args_raw'] = list(struct.unpack('<QQ',data[8:24]))
                active.append((depth,path))
            flat.append(item)
    return keys,flat


def token_facts(meta, flat):
    facts = {}
    for side in ('pre','post'):
        for i,row in enumerate(meta[side+'TokenBalances']):
            idx = row['accountIndex']
            fact = facts.setdefault(idx, {'sources':[]})
            for key,val in [('mint',row['mint']),('owner',row.get('owner')),('decimals',row['uiTokenAmount']['decimals']),('program',row.get('programId'))]:
                if val is None: raise ValueError('missing_token_'+key)
                if key in fact and fact[key] != val: raise ValueError('changed_token_identity')
                fact[key] = val
            amount = row['uiTokenAmount']['amount']
            if not isinstance(amount,str) or not amount.isdigit(): raise ValueError('bad_raw_token_amount')
            if side in fact: raise ValueError('duplicate_token_balance')
            fact[side] = int(amount)
            fact['sources'].append(f'meta.{side}TokenBalances[{i}]')
    for ix in flat:
        data,ac = ix['data'],ix['account_indices']
        if ix['program'] not in (TOKEN,TOKEN22) or not data: continue
        if data[0] in (1,16,18):
            owner = ix['accounts'][2] if data[0]==1 else b58encode(data[1:33])
            if data[0]!=1 and len(data)!=33: raise ValueError('bad_initialize_owner')
            fact=facts.setdefault(ac[0], {'sources':[]})
            for key,val in [('mint',ix['accounts'][1]),('owner',owner),('program',ix['program'])]:
                if key in fact and fact[key]!=val: raise ValueError('init_token_identity_conflict')
                fact[key]=val
            fact['initialized']=True
            fact['sources'].append(ix['path'])
        elif data[0]==9:
            fact=facts.setdefault(ac[0],{'sources':[]})
            fact['closed']=True
            fact['close_destination']=ix['accounts'][1]
            fact['sources'].append(ix['path'])
    for f in facts.values():
        if f.get('mint') == WSOL: f['decimals']=9
    return facts


def transfers(flat, facts):
    out=[]
    for ix in flat:
        data,ac=ix['data'],ix['account_indices']
        if ix['program'] not in (TOKEN,TOKEN22) or not data: continue
        if data[0] not in (3,12): continue
        checked=data[0]==12
        if len(data)!=(10 if checked else 9): raise ValueError('bad_transfer_length')
        source,dest=ac[0],ac[2 if checked else 1]
        sf,df=facts.get(source,{}),facts.get(dest,{})
        mint=ix['accounts'][1] if checked else sf.get('mint',df.get('mint'))
        decimals=data[9] if checked else sf.get('decimals',df.get('decimals'))
        for f in (sf,df):
            if f.get('mint',mint)!=mint or f.get('decimals',decimals)!=decimals:
                raise ValueError('transfer_mint_decimals_conflict')
        out.append({'path':ix['path'],'parent_swap':ix['parent_swap'], 'mint':mint,'decimals':decimals,
                    'amount_raw':int.from_bytes(data[1:9],'little'), 'source':ix['accounts'][0],
                    'destination':ix['accounts'][2 if checked else 1],
                    'authority':ix['accounts'][3 if checked else 2], 'program':ix['program']})
    return out
