package utils

import (
	"math/rand/v2"

	"github.com/nbd-wtf/go-nostr"
)

func RandomEvent() *nostr.Event {
	return &nostr.Event{
		CreatedAt: nostr.Timestamp(rand.Int64()),
		Kind:      rand.Int(),
		Tags:      RandomSlice(100, RandomTag),
	}
}

func RandomFilter() *nostr.Filter {
	since := nostr.Timestamp(rand.Int64())
	until := nostr.Timestamp(rand.Int64())
	return &nostr.Filter{
		IDs:     RandomSlice(100, RandomString64),
		Authors: RandomSlice(100, RandomString64),
		Kinds:   RandomSlice(100, rand.Int),
		Tags:    RandomTagMap(100),
		Since:   &since,
		Until:   &until,
		Limit:   rand.IntN(1000),
		Search:  RandomString(45),
	}
}

func RandomTagMap(max int) nostr.TagMap {
	size := rand.IntN(max)
	m := make(nostr.TagMap, size)
	for range size {
		m[RandomString(3)] = RandomTag()
	}
	return m
}

// RandomSlice returns a slice of random lenght up-to `max`, where each element
// is generated using the provided `genfunc`.
func RandomSlice[T any](max int, genFunc func() T) []T {
	n := rand.IntN(max)
	slice := make([]T, n)
	for i := range slice {
		slice[i] = genFunc()
	}
	return slice
}

func RandomTag() nostr.Tag {
	l := rand.IntN(8)
	tag := make(nostr.Tag, l)
	for i := range l {
		length := rand.IntN(10)
		tag[i] = RandomString(length)
	}
	return tag
}

const symbols = `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789`

func RandomString(l int) string {
	b := make([]byte, l)
	for i := range b {
		b[i] = symbols[rand.IntN(len(symbols))]
	}
	return string(b)
}

func RandomString64() string { return RandomString(64) }
