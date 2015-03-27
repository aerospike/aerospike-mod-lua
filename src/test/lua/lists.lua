--
-- Functions to support tests of list operations.
--

function create(rec,...)
-- Return a list of the args.
	local len = select('#',...)
	local l = list.create(len)
	if l == nil then
		warn("list creation failed")
	else
		for i = 1, len do
			l[i] = select(i,...)
		end
	end
	return l
end

function size(rec, bname)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	return list.size(l)
end

function iterate(rec, bname)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
--  Create a list that looks just like the input one.
	local l2 = list.create(list.size(l))
	local i = 1
	for v in list.iterator(l) do
		l2[i] = v
		i = i + 1
	end
	return l2
end

function insert(rec, bname, pos, val)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	list.insert(l, pos, val)
	return l
end

function append(rec, bname, val)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	list.append(l, val)
	return l
end

function prepend(rec, bname, val)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	list.prepend(l, val)
	return l
end

function take(rec, bname, n)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	return list.take(l, n)
end

function remove(rec, bname, pos)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	list.remove(l, pos)
	return l
end

function drop(rec, bname, n)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	return list.drop(l, n)
end

function trim(rec, bname, pos)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	list.trim(l, pos)
	return l
end

function clone(rec, bname)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	return list.clone(l)
end

function concat(rec, bname, bname2)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	local l2 = rec[bname2]
	if l2 == nil then
		warn("nothing found in bin %s", bname2)
	end
	list.concat(l, l2)
	return l
end

function merge(rec, bname, bname2)
	local l = rec[bname]
	if l == nil then
		warn("nothing found in bin %s", bname)
	end
	local l2 = rec[bname2]
	if l2 == nil then
		warn("nothing found in bin %s", bname2)
	end
	return list.merge(l, l2)
end

