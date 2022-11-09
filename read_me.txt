esempio del JSON da inviare in payload con metodo GET al server:

{
  "query": {
    "activityTypes": [
      "Taking Medicine",
      "Clean Up",
      "Watering Plants"
    ],
    "dateRange": [			#momentaneamente tratta solo array di lunghezza 1
      {
        "dateStart": "2016-01-01",
        "dateEnd": "2016-01-06"
      }
    ],
    "timeRange": [			#momentaneamente tratta solo array di lunghezza 1
      {
        "timeStart": "12:00:00",
        "timeEnd": "15:00:00"
      }
    ]
  }
