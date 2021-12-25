package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type User struct {
	Id   int
	Name string
}

func Test_SHOULD_ADD_USER(t *testing.T) {

	SetSqlLiteDB("test.db")
	sqllite, err := GetSqlLiteInstance()
	if err != nil {
		t.Error("cannot initizite table")
	}
	sqllite.AutoMigrate(&User{})
	user := User{Name: "Jinzhu", Id: 1}
	sqllite.Create(&user)
	var dbUser User
	result := sqllite.Find(&dbUser, user.Id)
	if result.Error != nil {
		t.Error("cannot query table")
	}
	assert.Equal(t, dbUser.Name, user.Name, "should dbuser and user names eq")

}
