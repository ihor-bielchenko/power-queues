
export const XAddBulk = `
	local UNPACK = table and table.unpack or unpack

	local stream = KEYS[1]
	local maxlen = tonumber(ARGV[1])
	local approxFlag = tonumber(ARGV[2]) == 1
	local n = tonumber(ARGV[3])
	local exactFlag = tonumber(ARGV[4]) == 1
	local nomkstream = tonumber(ARGV[5]) == 1
	local trimLimit = tonumber(ARGV[6])
	local minidWindowMs = tonumber(ARGV[7]) or 0
	local minidExact = tonumber(ARGV[8]) == 1
	local idx = 9
	local out = {}

	local common_opts = {}
	local co_len = 0

	if nomkstream then
		co_len = co_len + 1; common_opts[co_len] = 'NOMKSTREAM'
	end

	if minidWindowMs > 0 then
		local tm = redis.call('TIME')
		local now_ms = (tonumber(tm[1]) * 1000) + math.floor(tonumber(tm[2]) / 1000)
		local cutoff_ms = now_ms - minidWindowMs
		if cutoff_ms < 0 then cutoff_ms = 0 end
		local cutoff_id = tostring(cutoff_ms) .. '-0'

		co_len = co_len + 1; common_opts[co_len] = 'MINID'
		co_len = co_len + 1; common_opts[co_len] = (minidExact and '=' or '~')
		co_len = co_len + 1; common_opts[co_len] = cutoff_id
		if trimLimit and trimLimit > 0 then
			co_len = co_len + 1; common_opts[co_len] = 'LIMIT'
			co_len = co_len + 1; common_opts[co_len] = trimLimit
		end
	elseif maxlen and maxlen > 0 then
		co_len = co_len + 1; common_opts[co_len] = 'MAXLEN'
		if exactFlag then
			co_len = co_len + 1; common_opts[co_len] = '='
		elseif approxFlag then
			co_len = co_len + 1; common_opts[co_len] = '~'
		end
		co_len = co_len + 1; common_opts[co_len] = maxlen
		if trimLimit and trimLimit > 0 then
			co_len = co_len + 1; common_opts[co_len] = 'LIMIT'
			co_len = co_len + 1; common_opts[co_len] = trimLimit
		end
	end

	for e = 1, n do
		local id = ARGV[idx]; idx = idx + 1
		local num_pairs = tonumber(ARGV[idx]); idx = idx + 1

		local a = {}
		local a_len = 0

		for i = 1, co_len do a_len = a_len + 1; a[a_len] = common_opts[i] end

		a_len = a_len + 1; a[a_len] = id

		for j = 1, (num_pairs * 2) do
			a_len = a_len + 1; a[a_len] = ARGV[idx]; idx = idx + 1
		end

		local addedId = redis.call('XADD', stream, UNPACK(a))
		out[#out+1] = addedId or ''
	end

	return out
`;

export const Approve = `
	local stream = KEYS[1]
	local group = ARGV[1]
	local delFlag = tonumber(ARGV[2]) == 1

	local acked = 0
	local nids = #ARGV - 2
	if nids > 0 then
		acked = tonumber(redis.call('XACK', stream, group, unpack(ARGV, 3))) or 0
		if delFlag and nids > 0 then
			local ok, deln = pcall(redis.call, 'XDEL', stream, unpack(ARGV, 3))
			if not ok then
				deln = 0
				for i = 3, #ARGV do
					deln = deln + (tonumber(redis.call('XDEL', stream, ARGV[i])) or 0)
				end
			end
		end
	end
	return acked
`;

export const IdempotencyAllow = `
	local doneKey = KEYS[1]
	local lockKey = KEYS[2]
	local startKey = KEYS[3]

	if redis.call('EXISTS', doneKey) == 1 then
		return 1
	end

	local ttl = tonumber(ARGV[1]) or 0
	if ttl <= 0 then return 0 end

	local ok = redis.call('SET', lockKey, ARGV[2], 'NX', 'PX', ttl)
	if ok then
		if startKey and startKey ~= '' then
			redis.call('SET', startKey, 1, 'PX', ttl)
		end
		return 2
	else
		return 0
	end
`;

export const IdempotencyStart = `
	local lockKey = KEYS[1]
	local startKey = KEYS[2]
	if redis.call('GET', lockKey) == ARGV[1] then
		local ttl = tonumber(ARGV[2]) or 0
		if ttl > 0 then
			redis.call('SET', startKey, 1, 'PX', ttl)
			redis.call('PEXPIRE', lockKey, ttl)
		else
			redis.call('SET', startKey, 1)
		end
		return 1
	end
	return 0
`;

export const IdempotencyDone = `
	local doneKey  = KEYS[1]
	local lockKey  = KEYS[2]
	local startKey = KEYS[3]

	redis.call('SET', doneKey, 1)

	local ttlMs = tonumber(ARGV[1]) or 0
	if ttlMs > 0 then
		redis.call('PEXPIRE', doneKey, ttlMs)
	end

	if redis.call('GET', lockKey) == ARGV[2] then
		redis.call('DEL', lockKey)
		if startKey then
			redis.call('DEL', startKey)
		end
	end

	return 1
`;


export const IdempotencyFree = `
	local lockKey  = KEYS[1]
	local startKey = KEYS[2]
	if redis.call('GET', lockKey) == ARGV[1] then
		redis.call('DEL', lockKey)
		if startKey then redis.call('DEL', startKey) end
		return 1
	end
	return 0
`;

export const SelectStuck = `
	local stream = KEYS[1]
	local group = ARGV[1]
	local consumer = ARGV[2]
	local pendingIdleMs = tonumber(ARGV[3])
	local count = tonumber(ARGV[4]) or 0
	if count < 1 then count = 1 end

	local timeBudgetMs = tonumber(ARGV[5]) or 15
	local t0 = redis.call('TIME')
	local start_ms = (tonumber(t0[1]) * 1000) + math.floor(tonumber(t0[2]) / 1000)

	local results = {}
	local collected = 0
	local start_id = '0-0'
	local iters = 0
	local max_iters = math.max(16, math.ceil(count / 100))

	local function time_exceeded()
		local t1 = redis.call('TIME')
		local now_ms = (tonumber(t1[1]) * 1000) + math.floor(tonumber(t1[2]) / 1000)
		return (now_ms - start_ms) >= timeBudgetMs
	end

	while (collected < count) and (iters < max_iters) do
		local to_claim = count - collected
		if to_claim < 1 then break end

		local claim = redis.call('XAUTOCLAIM', stream, group, consumer, pendingIdleMs, start_id, 'COUNT', to_claim)
		iters = iters + 1

		local bucket = nil
		if claim then
			bucket = claim[2]
		end
		if bucket and #bucket > 0 then
			for i = 1, #bucket do
				results[#results+1] = bucket[i]
			end
			collected = #results
		end

		local next_id = claim and claim[1] or start_id
		if next_id == start_id then
			local s, seq = string.match(start_id, '^(%d+)%-(%d+)$')
			if s and seq then
				start_id = s .. '-' .. tostring(tonumber(seq) + 1)
			else
				start_id = '0-1'
			end
		else
			start_id = next_id
		end

		if time_exceeded() then
			break
		end
	end

	local left = count - collected
	if left > 0 then
		local xr = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', left, 'STREAMS', stream, '>')
		if xr and xr[1] and xr[1][2] then
			local entries = xr[1][2]
			for i = 1, #entries do
				results[#results+1] = entries[i]
			end
		end
	end

	return results
`;
