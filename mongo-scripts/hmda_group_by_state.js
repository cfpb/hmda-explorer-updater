db.hmda_lar_by_state.drop();

db.hmda_lar.aggregate([
  // { $limit: 1000000 },
  { "$group": {
      "_id": "$state_code",
      "purchases13": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2013 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "improvements13": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2013 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 2 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "refinances13": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2013 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 3 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "purchases14": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2014 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "improvements14": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2014 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 2 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "refinances14": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2014 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 3 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "purchases15": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2015 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "improvements15": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2015 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 2 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "refinances15": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2015 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 3 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "conv13": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2013 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 1 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "fha13": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2013 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 2 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "va13": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2013 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 3 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "rhs13": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2013 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 4 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "conv14": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2014 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 1 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "fha14": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2014 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 2 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "va14": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2014 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 3 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "rhs14": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2014 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 4 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "conv15": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2015 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 1 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "fha15": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2015 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 2 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "va15": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2015 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 3 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "rhs15": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2015 ] },
                      { "$eq": [ "$action_taken", 1 ] },
                      { "$eq": [ "$loan_purpose", 1 ] },
                      { "$eq": [ "$loan_type", 4 ] },
                      { "$eq": [ "$lien_status", 1 ] },
                      { "$eq": [ "$owner_occupancy", 1 ] },
                      { "$lte": [ "$property_type", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      }
  }},
  { $out: "hmda_lar_by_state" }
]);
