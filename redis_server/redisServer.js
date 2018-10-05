var mysql = require("mysql");
var redis = require("redis");
var client = redis.createClient();

///PERSONNAL FUNCTION/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//first possibility
function updateRedis(arrKeys) {
    for (var i = 0; i < arrKeys.length; i++) {
        insertInRedis = JSON.stringify(arrKeys[i]);
        client.set(arrKeys[i].crc_dom, insertInRedis);
    }
    console.log("Insertion in Redis done");
}

//second possibility
/*function updateRedis(arrKeys) {
    for (var i = 0; i < arrKeys.length; i++) {
        var tab = arrKeys[i].client_id.split(',');
        for (var j = 0; j < tab.length; j++) {
            var obj = {
                [tab[j]]: []
            }
            obj = JSON.stringify(obj)
            client.rpush(arrKeys[i].crc_dom, obj);
        }
    }
    console.log('Insertion in Redis done');
}*/

//third possiblity
/*function updateRedis(arrKeys) {
    for (var i = 0; i < arrKeys.length; i++) {
        var tab = arrKeys[i].client_id.split(',');
        for (var j = 0; j < tab.length; j++) {
            var obj = {
                [tab[j]]: []
            }
            var range = tab[j];
            obj = JSON.stringify(obj)
            client.ZADD(arrKeys[i].crc_dom, range, obj);
        }
    }
    console.log('Insertion in Redis done');
}*/

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////'

var query1 = "SELECT id FROM TABLE";

function query2(val) {
    return (
        " INSERT IGNORE INTO yourDatabase" +
        " (fw_url, crc_dom, domain, fw_flag, fw_cat, num_id, id_value, client_id)" +
        " SELECT af.fw_url, " +
        ' crc32(SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX(replace( replace(replace(SUBSTRING_INDEX( af.fw_url, "%", 2 ), "%", ""), "https://", ""), "http://", ""), "/", 1 ), "www.", -1 ), ".", 1 ))' +
        "  AS crc_dom," +
        ' SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX(replace( replace(replace(SUBSTRING_INDEX( af.fw_url, "%", 2 ), "%", ""), "https://", ""), "http://", ""), "/", 1 ), "www.", -1 ), ".", 1 )' +
        " AS domain, " +
        " af.fw_flag, af.fw_cat, af.num_id, af.id_value," +
        " c.id AS client_id" +
        " FROM admin_firewall af" +
        " JOIN admin_alertes_categories aa ON af.fw_cat = aa.cat_code" +
        " JOIN admin_categorie_pour_client_zend az ON az.code_alerte_categorie = aa.cat_id" +
        " JOIN client c ON az.id_client = c.id" +
        " WHERE c.id =" + val + "" +
        " ORDER BY client_id" +
        " LIMIT 5;"
    );
}

var query3 =
    "SELECT crc_dom, fw_url, fw_cat, GROUP_CONCAT(DISTINCT client_id SEPARATOR ',') as client_id" +
    " FROM yourDatabase GROUP BY crc_dom, fw_url, fw_cat";

var mysqlServers = mysql.createConnection({
    host: "localhost",
    user: "root",
    database: "yourDatabase",
    password: "yourPassword",
    debug: ["ComQueryPacket"]
});
mysqlServers.connect(function(err) {
    if (err) console.error("ERROR CONNECTION", err.code);
});
mysqlServers.query(query1, function(err, result) {
    if (err) {
        console.error(err);
    } else {
        var restReq = result.length,
            totalReq = result.length;
        for (var i = 0; i < result.length; i++) {
            mysqlServers.query(query2(result[i].id), function(err, res) {
                if (err) {
                    console.error(err);
                } else {
                    console.log("Total request ---> ", totalReq);
                    console.log("Remaining requests ---> ", restReq);
                    restReq--;
                    if (restReq === 0) {
                        mysqlServers.query(query3, function(err, res) {
                            if (err) {
                                console.error(err);
                            } else {
                                updateRedis(res);
                            }
                        });
                    }
                }
            });
        }
    }
});
