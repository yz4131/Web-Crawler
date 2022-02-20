import sqlite3

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

cur.execute('SELECT DISTINCT from_id FROM Links')
from_ids = list()
for row in cur:
    from_ids.append(row[0])

to_ids = list()
links = list()
cur.execute('SELECT DISTINCT from_id,to_id FROM Links')
for row in cur:
    from_id=row[0]
    to_id=row[1]
    if from_id==to_id:
        continue
    if to_id not in from_ids :
        continue
    links.append(row)
    if to_id not in to_ids :
        to_ids.append(to_id)

# Get latest page ranks
prev_ranks = dict()
for id in from_ids:
    cur.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (id, ))
    row = cur.fetchone()
    prev_ranks[id] = row[0]

sval = input('How many iterations:')
many=1
if ( len(sval) > 0 ) :
    many = int(sval)

for i in range(many):
    next_ranks = dict();
    total = 0.0
    for id, old_rank in prev_ranks.items():
        total = total + old_rank
        next_ranks[id] = 0.0

    # Find the number of outbound links and sent the page rank down each
    for id,old_rank in prev_ranks.items():
        give_ids = list()
        for from_id, to_id in links:
            if from_id != id :
                continue
            give_ids.append(to_id)
        if ( len(give_ids) < 1 ) :
            continue
        amount = old_rank / len(give_ids)

        for give_id in give_ids:
            next_ranks[give_id] = next_ranks[give_id] + amount

    newtot = 0
    for id, next_rank in next_ranks.items():
        newtot = newtot + next_rank
    evap = (total - newtot) / len(next_ranks)
    print('evap',evap)
    # print newtot, evap
    for id in next_ranks:
        next_ranks[id] = next_ranks[id] + evap

    # Compute the per-page average change from old rank to new rank
    # As indication of convergence of the algorithm
    totdiff = 0
    for id, old_rank in prev_ranks.items():
        new_rank = next_ranks[id]
        diff = abs(old_rank-new_rank)
        totdiff = totdiff + diff

    avediff = totdiff / len(prev_ranks)
    print(i+1, avediff)

    # rotate
    prev_ranks = next_ranks

# Put the final ranks back into the database
print(list(next_ranks.items())[:5])
cur.execute('''UPDATE Pages SET old_rank=new_rank''')
for id, new_rank in next_ranks.items() :
    cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))
conn.commit()
cur.close()
