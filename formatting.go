package main

/*import (
	"encoding/json"
	"os"
	"strings"
	"time"

	//important: must execute first; do not move
	_ "github.com/KristinaEtc/slflog"
	_ "github.com/lib/pq"
)

// There are 2 global types for elastic mapping: "geo" and "logs".
// According to this types we use different functions for validating and formatting messages.

// FormatMsg choose on of those formats
func FormatMsg(msg []byte, typeName string) (*string, *string, error) {

	var message, indexDate *string
	var err = error.New("Unknown typeName; msg ignoring")

	if strings.Contains(typeName, "geo") {
		message, indexDate, err = formatMsgGeo(msg)
	} else if strings.Contains(typeName, "logs") {
		message, indexDate, err = formatMsgLogs(msg)
	}

	log.Warn(")
    return message, indexDate, err

}
func formatMsgGeo(msg []byte) (*string, *string, error) {
	log.Warn(" formatMsgGeo")
	os.Exit(1)
	return nil, nil, nil
}

func formatMsgLogs(msg []byte) (*string, *string, error) {

	// unmarshal check if msg has nessesary fields
	importantFields, err := checkMsgForValid(msg)
	if err != nil {
		return nil, nil, err
	}

	lenUTCFormat := len((*importantFields).Utc)
	var timeStr string
	var t time.Time

	switch {
	case lenUTCFormat == lenRfc3339:
		{
			t, err = time.Parse(time.RFC3339, (*importantFields).Utc)
		}
	case lenUTCFormat < lenRfc3339:
		{
			if lenUTCFormat == lenRfc3339WithoutZ {
				t, err = time.Parse("2006-01-02T15:04:05", (*importantFields).Utc)
			}
			if lenUTCFormat == lenRfc3339WithZ {
				t, err = time.Parse("2006-01-02T15:04:05Z", (*importantFields).Utc)
			}
		}
	case lenUTCFormat > lenRfc3339:
		{
			if lenUTCFormat == lenRfc3339WithTimeZone {
				t, err = time.Parse("2006-01-02T15:04:05Z +0300 MSK", (*importantFields).Utc)
			}
		}
	default:
		{
			log.Warnf("Unknown utc format: %s", (*importantFields).Utc)
			err = ErrUnknownUTCFormat
		}
	}

	if err != nil {
		return nil, nil, err
	}

	timeStr = t.String()

	//cut all except date
	idx := strings.IndexAny(timeStr, " ")
	timeStr = timeStr[:idx]

	formattedMsg := addProcessNameShort(msg, (*importantFields).ProcessName)
	deleteNewlineSym(&formattedMsg)

	return &formattedMsg, &timeStr, nil
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
*/
