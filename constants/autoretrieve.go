package constants

const QueryNewCIDs string = "select objects.cid from objects left join obj_refs on objects.id = obj_refs.object where obj_refs.content in (select id from contents where created_at > ?);"

const KeyToCidMapPrefix = "map/keyCid/"
const CidToKeyMapPrefix = "map/cidKey/"
const KeyToMetadataMapPrefix = "map/keyMD/"
const LatestAdvKey = "sync/adv/"
const LinksCachePath = "/cache/links"
