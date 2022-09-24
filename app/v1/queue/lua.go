package queue

const Pending2ReadyScript = `
--KEYS[1] zset 
--KEYS[2] list
--ARGV[1] timestamp
--ARGV[2] limit 

--get ready msg
local val = redis.call('ZRANGE', KEYS[1], 0, ARGV[1], 'BYSCORE', 'LIMIT', ARGV[2])
if(next(val) ~= nil) then 
	redis.call('ZREMRANGEBYRANK',KEYS[1], 0, #val - 1)
	redis.call('LPUSH', KEYS[2], unpack(val,1,#val))
end
return #val
`
