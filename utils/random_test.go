package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Should_create_16_length_of_random_string(t *testing.T) {

	str := RandomString(16)
	assert.Equal(t, len(str), 16)
}
