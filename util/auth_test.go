package util

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIsCollectionOwner(t *testing.T) {
	require.Nil(t, IsCollectionOwner(290, 290))
	assert.Equal(t, IsCollectionOwner(1, 2).Error(), "ERR_NOT_AUTHORIZED: User (1) is not authorized for collection (2)")
}

func TestIsContentOwner(t *testing.T) {
	require.Nil(t, IsContentOwner(290, 290))
	assert.Equal(t, IsContentOwner(1, 2).Error(), "ERR_NOT_AUTHORIZED: User (1) is not authorized for content (2)")
}
