sudo docker run -d \
	--name some-postgres \
	--restart always \
	-e POSTGRES_PASSWORD=exemplosenha123 \
	-e PGDATA=/var/lib/postgresql/data/pgdata \
	-v /custom/mount:/var/lib/postgresql/data \
	-p 5432:5432 \
	postgres


sudo docker update \
	--name some-postgres \
	--restart always \
	-e POSTGRES_PASSWORD=exemplosenha123 \
	-e PGDATA=/var/lib/postgresql/data/pgdata \
	-v /custom/mount:/var/lib/postgresql/data \
	-p 5432:5432 \
	postgres

