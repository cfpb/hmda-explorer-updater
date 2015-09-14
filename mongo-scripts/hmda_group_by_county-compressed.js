// We're compressing the keys because TileMill sometimes truncates them and gets confused.
// They're in the format: loanpurpose_actiontaken_year. For example:
//
// p_a_12 is purchase applications in 2012.
// p_o_12 is purchase originations in 2012.
// r_a_12 is refinance applications in 2012.
//

db.hmda_lar_by_county.drop();

db.hmda_lar.aggregate([
  // { $limit: 100000 },
  { "$group": {
      "_id": { state_code: "$89", county_code: "$d1" },
      "p_a_12": { 
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$da", 2012 ] },
                      { "$eq": [ "$809", 1 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "p_o_12": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$da", 2012 ] },
                      { "$eq": [ "$fb", 1 ] },
                      { "$eq": [ "$809", 1 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "p_a_13": { 
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$da", 2013 ] },
                      { "$eq": [ "$809", 1 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
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
                      { "$eq": [ "$da", 2013 ] },
                      { "$eq": [ "$fb", 1 ] },
                      { "$eq": [ "$809", 1 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
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
                      { "$eq": [ "$da", 2014 ] },
                      { "$eq": [ "$809", 1 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
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
                      { "$eq": [ "$da", 2014 ] },
                      { "$eq": [ "$fb", 1 ] },
                      { "$eq": [ "$809", 1 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "r_a_12": { 
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$da", 2012 ] },
                      { "$eq": [ "$809", 3 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
                  ]},
                  1,
                  0
              ]
          }
      },
      "r_o_12": {
          "$sum": {
              "$cond": [
                  { "$and": [
                      { "$eq": [ "$da", 2012 ] },
                      { "$eq": [ "$fb", 1 ] },
                      { "$eq": [ "$809", 3 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
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
                      { "$eq": [ "$da", 2013 ] },
                      { "$eq": [ "$809", 3 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
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
                      { "$eq": [ "$da", 2013 ] },
                      { "$eq": [ "$fb", 1 ] },
                      { "$eq": [ "$809", 3 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
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
                      { "$eq": [ "$da", 2014 ] },
                      { "$eq": [ "$809", 3 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
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
                      { "$eq": [ "$da", 2014 ] },
                      { "$eq": [ "$fb", 1 ] },
                      { "$eq": [ "$809", 3 ] },
                      { "$eq": [ "$25", 1 ] },
                      { "$eq": [ "$a14", 1 ] },
                      { "$lte": [ "$eb", 2 ] }
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
      p_a_12: "$p_a_12",
      p_a_13: "$p_a_13",
      p_a_14: "$p_a_14",
      p_o_12: "$p_o_12",
      p_o_13: "$p_o_13",
      p_o_14: "$p_o_14",
      r_a_12: "$r_a_12",
      r_a_13: "$r_a_13",
      r_a_14: "$r_a_14",
      r_o_12: "$r_o_12",
      r_o_13: "$r_o_13",
      r_o_14: "$r_o_14",
      change_p_o_12_13: { $multiply: [{ $divide: [{ $subtract: ["$p_o_13", "$p_o_12"]}, { $cond: [ { $gt: [ "$p_o_12", 0 ] }, "$p_o_12", 1 ] }] }, 100 ] },
      change_p_o_13_14: { $multiply: [{ $divide: [{ $subtract: ["$p_o_14", "$p_o_13"]}, { $cond: [ { $gt: [ "$p_o_13", 0 ] }, "$p_o_13", 1 ] }] }, 100 ] },
      change_p_a_12_13: { $multiply: [{ $divide: [{ $subtract: ["$p_a_13", "$p_a_12"]}, { $cond: [ { $gt: [ "$p_a_12", 0 ] }, "$p_a_12", 1 ] }] }, 100 ] },
      change_p_a_13_14: { $multiply: [{ $divide: [{ $subtract: ["$p_a_14", "$p_a_13"]}, { $cond: [ { $gt: [ "$p_a_13", 0 ] }, "$p_a_13", 1 ] }] }, 100 ] },
      change_r_o_12_13: { $multiply: [{ $divide: [{ $subtract: ["$r_o_13", "$r_o_12"]}, { $cond: [ { $gt: [ "$r_o_12", 0 ] }, "$r_o_12", 1 ] }] }, 100 ] },
      change_r_o_13_14: { $multiply: [{ $divide: [{ $subtract: ["$r_o_14", "$r_o_13"]}, { $cond: [ { $gt: [ "$r_o_13", 0 ] }, "$r_o_13", 1 ] }] }, 100 ] },
      change_r_a_12_13: { $multiply: [{ $divide: [{ $subtract: ["$r_a_13", "$r_a_12"]}, { $cond: [ { $gt: [ "$r_a_12", 0 ] }, "$r_a_12", 1 ] }] }, 100 ] },
      change_r_a_13_14: { $multiply: [{ $divide: [{ $subtract: ["$r_a_14", "$r_a_13"]}, { $cond: [ { $gt: [ "$r_a_13", 0 ] }, "$r_a_13", 1 ] }] }, 100 ] }
    }
  },
  { $out: "hmda_lar_by_county" }
]);
