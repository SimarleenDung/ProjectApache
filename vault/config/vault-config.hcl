storage "raft" {
  path    = "/vault/data"
  node_id = "vault_1"
}
 
listener "tcp" {
  address         = "0.0.0.0:8200"
  cluster_address = "vault:8201"   # <-- no http:// here
  tls_disable     = 1
}
 
ui = true
disable_mlock = true
 
api_addr     = "http://localhost:8200"
cluster_addr = "http://vault:8201"