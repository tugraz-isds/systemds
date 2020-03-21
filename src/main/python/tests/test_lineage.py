from systemds.matrix import full
m = full((5, 10), 4.20)
m_res = m * 3.1
m_sum = m_res.sum()

#print lineage trace of intermediate m_res
print(m_res.getLineageTrace())

#compute and print lineage trace of the full dag lazily
print(m_sum.compute(verbose = True, lineage = True))
