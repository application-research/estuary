package node

import (
	"fmt"
	"testing"
)

func TestBsCfg(t *testing.T) {
	str := ":flatfs(4,5,7):/beep/boop"

	tp, params, p, err := parseBsCfg(str)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(tp, params, p)

	str = ":lmdbs:/beep/boop"

	tp, params, p, err = parseBsCfg(str)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(tp, params, p)

	str = ":migrate(:badger:/bats,:flatfs:/bear):/beep/boop"

	tp, params, p, err = parseBsCfg(str)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(tp, params, p)
}
