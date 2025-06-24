package ephemeral

import (
	"testing"

	"github.com/pippellia-btc/nastro"
)

func TestInterface(t *testing.T) {
	var _ nastro.Store = &Ephemeral{}
}
