
function bytes_create(r, b )
--    info("bytes create called")
    local x = bytes(15)
--    info(" bytes to string is %s",tostring(x))
    local y = bytes.size(x)
    return y
end

function get_set_integer(r, b) 
--    info("get set integer test")

    local x = bytes(16)

    bytes.put_int16(x, 0, 4)
    bytes.put_int16(x, 2, 8)
    bytes.put_int16(x, 4, 0xffff)

--    info(" buffer is %s",tostring(x))

    local a1 = bytes.get_int16(x, 0)
    local a2 = bytes.get_int16(x, 2)

--    info("get16: position 0 %s",tostring(a1))
--    info("get16: position 2 %s",tostring(a2))

    return "OK"

end

function get_set_index(r, b) 
--    info("get set index test")

    local x = bytes(16)

    x[0] = 1
    x[1] = 2

    a1 = x[0]
    a2 = x[1]

--    info(" buffer is %s",tostring(x))

--    info("get16: position 0 %s",tostring(a1))
--    info("get16: position 1 %s",tostring(a2))

    return "OK"

end

