# create autoretrieve server
# curl -X POST -H "Authorization: Bearer $APIKEY" "localhost:3004/admin/autoretrieve/init" -d "privateKey=CAESQE6k3xeczE5%2BhmIB8Pt8SHA2c5YM0v3XBKeBHwXBsvP6NfsmsxQvnZZUMM054AiqQEWifDQSZi%2BxSlJMd4xT8GY%3D&addresses=/ip4/192.168.0.17/tcp/6744/p2p/12D3KooWSjcfenYRqMDy77dR9LpfnH5NFNJKqDxGHzMaqib7PUDA"

# upload new cid
# curl -X POST http://localhost:3004/content/add-ipfs -d '{ "name": "some-cid.txt", "root": "CID" }' -H "Content-Type: application/json" -H "Authorization: Bearer $APIKEY"

while [ true ]
do
    curl -X POST -H "Authorization: Bearer SECRET367ef629-bb84-495e-b26c-bb8fed9bb6e8SECRET" "localhost:3004/autoretrieve/heartbeat"| jq
    sleep $1;
done
