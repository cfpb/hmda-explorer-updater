db.hmda_lar_by_county.drop();

db.hmda_lar.aggregate([
  // { $limit: 1000000 },
  { "$group": {
      "_id": { state_code: "$state_code", county_code: "$county_code" },
      "p_a_13": { 
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2013 ] },
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
      "p_o_13": {
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
      "p_a_14": { 
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2014 ] },
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
      "p_o_14": {
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
      "p_a_15": { 
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2015 ] },
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
      "p_o_15": {
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
      "r_a_13": { 
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2013 ] },
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
      "r_o_13": {
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
      "r_a_14": { 
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2014 ] },
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
      "r_o_14": {
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
      "r_a_15": { 
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$as_of_year", 2015 ] },
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
      "r_o_15": {
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
      }
  }},
  {
    $project: {
      _id: 0,
      state_code: "$_id.state_code",
      county_code: "$_id.county_code",
      p_a_13: "$p_a_13",
      p_a_14: "$p_a_14",
      p_a_15: "$p_a_15",
      p_o_13: "$p_o_13",
      p_o_14: "$p_o_14",
      p_o_15: "$p_o_15",
      r_a_13: "$r_a_13",
      r_a_14: "$r_a_14",
      r_a_15: "$r_a_15",
      r_o_13: "$r_o_13",
      r_o_14: "$r_o_14",
      r_o_15: "$r_o_15",
      change_p_o_13_14: { $multiply: [{ $divide: [{ $subtract: ["$p_o_14", "$p_o_13"]}, { $cond: [ { $gt: [ "$p_o_13", 0 ] }, "$p_o_13", 1 ] }] }, 100 ] },
      change_p_o_14_15: { $multiply: [{ $divide: [{ $subtract: ["$p_o_15", "$p_o_14"]}, { $cond: [ { $gt: [ "$p_o_14", 0 ] }, "$p_o_14", 1 ] }] }, 100 ] },
      change_p_a_13_14: { $multiply: [{ $divide: [{ $subtract: ["$p_a_14", "$p_a_13"]}, { $cond: [ { $gt: [ "$p_a_13", 0 ] }, "$p_a_13", 1 ] }] }, 100 ] },
      change_p_a_14_15: { $multiply: [{ $divide: [{ $subtract: ["$p_a_15", "$p_a_14"]}, { $cond: [ { $gt: [ "$p_a_14", 0 ] }, "$p_a_14", 1 ] }] }, 100 ] },
      change_r_o_13_14: { $multiply: [{ $divide: [{ $subtract: ["$r_o_14", "$r_o_13"]}, { $cond: [ { $gt: [ "$r_o_13", 0 ] }, "$r_o_13", 1 ] }] }, 100 ] },
      change_r_o_14_15: { $multiply: [{ $divide: [{ $subtract: ["$r_o_15", "$r_o_14"]}, { $cond: [ { $gt: [ "$r_o_14", 0 ] }, "$r_o_14", 1 ] }] }, 100 ] },
      change_r_a_13_14: { $multiply: [{ $divide: [{ $subtract: ["$r_a_14", "$r_a_13"]}, { $cond: [ { $gt: [ "$r_a_13", 0 ] }, "$r_a_13", 1 ] }] }, 100 ] },
      change_r_a_14_15: { $multiply: [{ $divide: [{ $subtract: ["$r_a_15", "$r_a_14"]}, { $cond: [ { $gt: [ "$r_a_14", 0 ] }, "$r_a_14", 1 ] }] }, 100 ] }
    }
  },
  { $out: "hmda_lar_by_county" }
]);
