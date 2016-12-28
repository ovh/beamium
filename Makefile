VERSION=$(shell cat Cargo.toml | awk '/version/{print $$NF}')

.PHONY: dev
dev:
	cargo build

.PHONY: release
release:
		cargo build --release

.PHONY: deb
deb: release
		fpm -m "<kevin@d33d33.fr>" \
		  --description "Prometheus to Warp10 metrics forwarder" \
			--url "https://github.com/runabove/beamium" \
			--license "BSD-3-Clause" \
			--version $(VERSION) \
			-n beamium \
			-d logrotate \
			-s dir -t deb \
			-a all \
			--deb-user beamium \
			--deb-group beamium \
			--deb-no-default-config-files \
			--config-files /etc/beamium/config.yaml \
			--deb-init packages/deb/beamium.init \
			--directories /opt/beamium \
			--directories /var/log/beamium \
			--before-install packages/deb/before-install.sh \
			--after-install packages/deb/after-install.sh \
			--before-remove packages/deb/before-remove.sh \
			--after-remove packages/deb/after-remove.sh \
			--inputs packages/deb/input
		mkdir -p target/packages
		mv *.deb target/packages/

.PHONY: clean
clean:
		rm -rf build
