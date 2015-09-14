function addCommas(x) {
  return x ? x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") : x;
}

db.hmda_lar_by_county.find().forEach(function(county) {
  db.hmda_lar_by_county.update({
    _id: county._id
  }, {
    $set: {
      p_a_12: addCommas(county.p_a_12),
      p_a_13: addCommas(county.p_a_13),
      p_a_14: addCommas(county.p_a_14),
      p_o_12: addCommas(county.p_o_12),
      p_o_13: addCommas(county.p_o_13),
      p_o_14: addCommas(county.p_o_14),
      r_a_12: addCommas(county.r_a_12),
      r_a_13: addCommas(county.r_a_13),
      r_a_14: addCommas(county.r_a_14),
      r_o_12: addCommas(county.r_o_12),
      r_o_13: addCommas(county.r_o_13),
      r_o_14: addCommas(county.r_o_14),
      change_p_o_12_13: Math.round(county.change_p_o_12_13),
      change_p_o_13_14: Math.round(county.change_p_o_13_14),
      change_p_a_12_13: Math.round(county.change_p_a_12_13),
      change_p_a_13_14: Math.round(county.change_p_a_13_14),
      change_r_o_12_13: Math.round(county.change_r_o_12_13),
      change_r_o_13_14: Math.round(county.change_r_o_13_14),
      change_r_a_12_13: Math.round(county.change_r_a_12_13),
      change_r_a_13_14: Math.round(county.change_r_a_13_14),
      population: addCommas(county.population)
    }
  })
});
