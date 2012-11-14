

function range(i,n)
    local x = 0
    return function()
        if x < n then
            x = x + 1
            return x
        else 
            return nil
        end
    end
end