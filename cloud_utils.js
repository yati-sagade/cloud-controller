const CHARS =     "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
               +  "abcdefghijklmnopqrstuvwxyz"
               +  "1234567890-_";

exports.randomClamped = function(hi, low){
    return lo + parseInt(Math.random() * ((hi - lo) + 1));
};

exports.randomString = function(length){
    ret = "";
    if(length <= 0){
        return ret;
    }
    for(var i = 0; i < length; i++){
        ret += CHARS.charAt(randomClamped(0, CHARS.length));
    }
    return ret;
};

// buEncode(obj) -> encodeURIComponent(base64_encode(str))
exports.buEncode = function(obj){
    var s;
    if(typeof obj !== "string")
        s = JSON.stringify(obj);
    else
        s = obj;

    var b = new Buffer(s).toString("base64");
    return encodeURIComponent(b);
};

exports.buDecode = function(s){
    var b = decodeURIComponent(s);
    var ret = JSON.parse(new Buffer(b, "base64").toString());
    return ret;
};