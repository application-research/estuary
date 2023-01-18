package pinner

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetPin(t *testing.T) {
	type args struct {
		param GetPinParam
	}
	tests := []struct {
		name    string
		args    args
		want    *types.IpfsPinStatusResponse
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetPin(tt.args.param)
			if !tt.wantErr(t, err, fmt.Sprintf("GetPin(%v)", tt.args.param)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetPin(%v)", tt.args.param)
		})
	}
}
