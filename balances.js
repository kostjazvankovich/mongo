db.accountBalanceView.drop()
db.runCommand(
{
  create: "accountBalanceView",
  viewOn: "bookedBalance",
  pipeline: [
    {$unwind: {path: "$saldenList"}},
    {
      $project: {
        clientId: "clientId",
        postenNr: "$saldenList.postenId.postenNr",
        assets: "$saldenList.details",
      }
    },
    { $unwind: {path: "$assets"} },
    {
  $project: {
  id: 0,
    clientId:1,
  postenNr: 1,
  bank: "$assets.displayText",
  }},
    
    { $unwind: {path: "$balances"} },
{
      $project:  {
        postenNr: 1,
        clientId: 1,
        bank: 1,
        day: "$balances.day",
        value: "$balances.saldoInCent"
      }
    },
    {
      $group: {
        _id: {
  clientId: "$clientId",
          postenNr: "$postenNr",
          day: {
            $dateFromParts: {
              year: { $year: "$day"},
              month: { $month: "$day"},
              day: { $dayOfMonth: "$day"}
            }
          }
        },
        banks: { $addToSet: "$bank"},
        total: { $sum: "$value"}
      }
    },
    {
      $facet: {
        banks: [
          { $match: {$or: [{ "_id.postenNr": 16730000 }, { "_id.postenNr": 25210000 }]}},
          {
            $group: {
              _id: "$_id.day",
  posten: { $addToSet: "$_id.postenNr"},
              total: {
                $sum: {
                  $cond: [
                    { $eq: [ "$_id.postenNr", 25210000 ] },
                    { "$subtract": [ 0, "$total" ] },
                    "$total"
                  ]
                }
              },
          {
            $project: {
              _id: 0,
  posten: 1,
              banks: 1,
              day: "$_id.day",
              total: 1,
            }
          },
          {
            $sort: { day: 1 }
          }
        ],
        debts: [
          { $match: { $or: [ { "_id.postenNr": 16730000 }, { "_id.postenNr": 25210000 }, { "_id.postenNr": 15610106 } ] } },
          {
            $group: {
              _id: "$_id.day"
  posten: { $addToSet: "$_id.postenNr"},
              total: {
                $sum: {
                  $cond: [
                    { $eq: [ "$_id.postenNr", 25210000 ] },
                    { "$subtract": [ 0, "$total" ] },
                    "$total"
                  ]
                }
              },
          {
            $project: {
              _id: 0,
  posten: 1,
              banks: 1,
              day: "$_id.day",
              total: 1,
            }
          },
          {
            $sort: { day: 1 }
          }
        ],
        demands: [
          { $match: { $or: [ { "_id.postenNr": 16730000 }, { "_id.postenNr": 25210000 }, { "_id.postenNr": 15610106 }, { "_id.postenNr": 25411700 } ] } },
          {
            $group: {
              _id: "$_id.day",
  posten: { $addToSet: "$_id.postenNr"},
              total: {
                $sum: {
                  $cond: [
                    {$or: [ { $eq: [ "$_id.postenNr", 25411700 ] }, { $eq: [ "$_id.postenNr", 25210000 ] } ]},
                    { "$subtract": [ 0, "$total" ] },
                    "$total"
                  ]
                }
              }
            }
          },
          {
            $project: {
  posten:1,
              _id: 0,
              day: "$_id",
              total: 1
            }
          },
          {
            $sort: { day: 1 }
          }
        ]
      }
    }
  ]
})
