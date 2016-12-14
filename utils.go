package main

import (
	"encoding/json"
	"strings"
)

func checkMsgForValid(msg []byte) (*NecessaryFields, error) {

	var data NecessaryFields

	if err := json.Unmarshal(msg, &data); err != nil {
		log.Errorf("Could not get parse request: %s", err.Error())
		return nil, err
	}
	if data.ID == "" || data.Type == "" || data.Utc == "" {
		log.Warnf("Not status message %s", string(msg))
		return nil, ErrNoNeedfullFields
	}
	return &data, nil
}

func addProcessNameShort(msg []byte, fullProcessName string) string {

	shortName := getShortName(fullProcessName)
	newMsg := "{\"process_short\": \"" + shortName + "\", " + string(msg[1:])
	return newMsg
}

func getShortName(fullName string) string {
	if fullName == "" {
		return ""
	}

	var shortName string

	shortName = trimStringFromSym(fullName, "/")
	if shortName != "" {
		return shortName
	}

	shortName = trimStringFromSym(fullName, "\\")
	if shortName != "" {
		return shortName
	}

	return fullName
}

func trimStringFromSym(str string, sym string) string {
	if idx := strings.LastIndex(str, sym); idx != -1 {
		return str[(idx-1)+2:]
	}
	return ""
}

func deleteNewlineSym(msg *string) {

	formattedMsg := *msg
	if strings.Contains(formattedMsg, "\n") {

		var msgMapTemplate interface{}
		err := json.Unmarshal([]byte(formattedMsg), &msgMapTemplate)
		if err != nil {
			log.WithField("func", "deleting newline in msg").Error(err.Error())
			return
		}
		msgMap := msgMapTemplate.(map[string]interface{})
		//for _, v := range msgMapTemplate {
		jsonMsg, err := json.Marshal(msgMap)
		if err != nil {
			log.WithField("func", "deleting newline in msg").Error(err.Error())
			return
		}

		*msg = string(jsonMsg)
	}

}
