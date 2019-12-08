db.transactionView.drop()
db.runCommand({
  create: "transactionView",
  viewOn: "Transactions",
  pipeline: [
    {
      $lookup:
      {
        from: "activeTransactionView",
        let: { bookingDate: "$bookingDate", accountId: "$accountId", clientId: "$clientId" },
        pipeline: [
          { $match:
            { $expr:
              {  $and: 
                 [
                   { $eq: [ "$clientId",  "$$clientId" ] },
                   { $eq: [ "$accountId",  "$$accountId" ] },
                 ]
              }
            }
          },
          { $project: { _id: 0 } }
        ],
        as: "activeTransactions"
      }
    },
    {
      $unwind: "$activeTransactions"
    },
    {
      $group:
      {
        _id:
        {
          clientId: "$clientId",
          accountId: "$accountId",
          bookingDate: "$bookingDate",
          bookingDateClosingBalance: "$bookingDateClosingBalance"
        },
      }
    },
    {
      $project:
      {
        _id: 0,
        clientId: "$_id.clientId",
        accountId: "$_id.accountId",
        bookingDate: "$_id.bookingDate",
        bookingDateClosingBalance: "$_id.bookingDateClosingBalance"
      }
    },
    {
      $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1}
    },
    {
      $group:
      {
        _id:
        {
          clientId: "$clientId",
          accountId: "$accountId"
        },
        startDate: { $first: "$bookingDate"},
        endDate: { $last: "$bookingDate"},
        bookingBalances: { $push: { bookingDate: "$bookingDate", bookingDateClosingBalance: "$bookingDateClosingBalance" }}
      }
    },
    {
      $project:
      {
        _id: 0,
        clientId: "$_id.clientId",
        accountId: "$_id.accountId",
        startDate: "$startDate",
        endDate: "$endDate",
        bookingBalances: "$bookingBalances"
      }
    },
    {
      $facet:
      {
        "rangeTransactions": [
          {
            $sort: { "endDate": -1 }
          },
          { $limit: 1 },
          {
            $project:
            {
              endBookingDate: "$endDate"
            }
          },
          {
            $addFields:
            {
              rangeDate:
              {
                $map:
                {
                  input:{$range:[0, 15*1000*60*60*24, 1000*60*60*24]},
                    in:{ $subtract:["$endBookingDate", "$$this"]}
                }
              }
            }
          },
          { $unwind: "$rangeDate"},
          {
            $project:
            {
              bookingDate: "$rangeDate"
            }
          },
          {
            $sort:{ "bookingDate": 1}
          },
        ],
        "lastTransactions": [
          {
            $project:
            {
              clientId: "$clientId",
              accountId: "$accountId",
              startBookingDate: "$startDate",
              endBookingDate: "$endDate",
              bookingBalances: "$bookingBalances"
            }
          }
        ]
      }
    },
    { $unwind: "$rangeTransactions" },
    { $unwind: "$lastTransactions" },
    {
      $project:
      {
        bookingDate: "$rangeTransactions.bookingDate",
        clientId: "$lastTransactions.clientId",
        accountId: "$lastTransactions.accountId",
        startBookingDate: "$lastTransactions.startBookingDate",
        endBookingDate: "$lastTransactions.endBookingDate",
        bookingDateClosingBalance:
        {
          $cond: [
            {
              $and: [
                {
                  $gte:  ["$rangeTransactions.bookingDate", "$lastTransactions.startBookingDate"]
                },
                {
                  $lte:  ["$rangeTransactions.bookingDate", "$lastTransactions.endBookingDate"]
                }
              ]
            },
            {
              $arrayElemAt: [
                {
                  $filter: {
                    "input": "$lastTransactions.bookingBalances",
                    "as": "balance",
                    "cond": {
                      "$and": [
                        {"$gte": ["$rangeTransactions.bookingDate", "$$balance.bookingDate"]}
                      ]
                    }}
                }, -1
              ]
            },
            {
              $cond: [
                {
                  $gt:  ["$rangeTransactions.bookingDate", "$lastTransactions.endBookingDate"]
                },
                {
                  $arrayElemAt: [ "$lastTransactions.bookingBalances", -1 ] 
                },
                {
                  $arrayElemAt: [ "$lastTransactions.bookingBalances", 0 ] 
                }
              ]
            }
          ]
        }
      }
    },
    {
      $group:
      {
        _id: 
        {
          clientId: "$clientId",
          bookingDate: "$bookingDate"
        },
        total: { $sum: "$bookingDateClosingBalance.bookingDateClosingBalance"}
      }
    },
    {
      $project: 
      {
        _id: 0,
        bookingDate: "$_id.bookingDate",
        clientId: "$_id.clientId",
        total: "$total"
      }
    },
    {
      $sort:{ "bookingDate": 1}
    }
  ]});
