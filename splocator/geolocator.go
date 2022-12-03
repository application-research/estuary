package splocator

import (
	"context"
	"log"
	"time"

	ipfsgeoip "github.com/hsanjuan/go-ipfs-geoip"
	ipfslite "github.com/hsanjuan/ipfs-lite"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
)

// Look up the locations associated with a list of Storage Providers
// If an SP's location cannot be determined, it will not be returned in the resulting list
//
// We make the assumption that it does not make sense if a miner reports IPs
// in multiple locations. Therefore, we take the first "resolved" location as
// the valid one.
func LookupLocations(ctx context.Context, loc *ipfsgeoip.IPLocator, sps []SP) ([]LocatedSP, error) {
	var locatedSPs []LocatedSP

	for i, sp := range sps {
		for _, addr := range sp.MultiAddresses {
			resolved, err := resolveMultiaddr(ctx, addr)
			if err != nil {
				continue
			}

			var lSP LocatedSP

			for _, r := range resolved {
				ipv4, errIP4 := r.ValueForProtocol(multiaddr.P_IP4)
				if errIP4 != nil {
					continue
				}

				geo, err := lookup(ctx, loc, ipv4)
				if err != nil {
					continue
				}
				if geo.CountryName == "" {
					// keep trying
					continue
				}

				lSP = LocatedSP{
					ID:             sp.ID,
					MultiAddresses: sp.MultiAddresses,
					CountryName:    geo.CountryName,
					CountryCode:    geo.CountryCode,
					Latitude:       geo.Latitude,
					Longitude:      geo.Longitude,
				}

				locatedSPs = append(locatedSPs, lSP)
				break
			}
			// We found a country. Move to next SP.
			if lSP.CountryName != "" {
				break // Break multiaddresses loop. Back to SPs loop
			}
		}
		if i%100 == 0 {
			log.Printf("Completed geo-lookup for %d out of %d (success on %d)", i+1, len(sps), len(locatedSPs))
		}
	}
	return locatedSPs, nil
}

func resolveMultiaddr(ctx context.Context, ma multiaddr.Multiaddr) ([]multiaddr.Multiaddr, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return madns.Resolve(ctx, ma)
}

func lookup(ctx context.Context, loc *ipfsgeoip.IPLocator, ip string) (ipfsgeoip.GeoIPInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return loc.Lookup(ctx, ip)
}

func getIpfsLocator(ctx context.Context) (*ipfsgeoip.IPLocator, error) {
	ds := ipfslite.NewInMemoryDatastore()
	priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		return nil, err
	}

	h, dht, err := ipfslite.SetupLibp2p(
		ctx,
		priv,
		nil,
		nil,
		ds,
		ipfslite.Libp2pOptionsExtra...,
	)

	if err != nil {
		return nil, err
	}

	bs := blockstore.NewBlockstore(ds)

	lite, err := ipfslite.New(ctx, ds, bs, h, dht, nil)
	if err != nil {
		return nil, err
	}

	go lite.Bootstrap(ipfslite.DefaultBootstrapPeers())

	// An exchange session will speed things up
	return ipfsgeoip.NewIPLocator(lite.Session(ctx)), nil
}
