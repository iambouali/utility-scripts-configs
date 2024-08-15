curl -s http://169.254.169.254/metadata/v1/id | xargs -I{} curl -X DELETE -H "Authorization: Bearer YOUR_API_TOKEN" "https://api.digitalocean.com/v2/droplets/{}"
