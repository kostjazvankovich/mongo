db.bankAccountView.drop()
db.runCommand({
  create: "bankAccountView",
  viewOn: "bankAccount",
  pipeline: [
    { $unwind: "$accountList"},
    {
      $group: {
        _id: {
          clientId: "$clientId",
          accountId: "$accountList.accountId"
        }
      }
    },
    { 
      $project: {
        _id: 0,
        clientId: "$_id.clientId",
        accountId: "$_id.accountId"
      }
    },
    { $lookup: 
      {
        from: "transactionView",
        let: { clientId: "$clientId", accountId: "$accountId"},
        pipeline: [
          { $match:
            { $expr:
              { $and:
                [
                  { $eq: [ "$clientId", "$$clientId"] },
                  { $eq: [ "$accountId", "$$accountId"] }
                ]
              }
            }
          },
          { 
            $project: 
            { 
              _id: 0,
            }
          }

        ],
        as: "balances"
      }
    },
    { 
      $unwind: "$balances"
    },
    {
      $group: {
        _id: {
          day: "$balances.bookingDate",
          clientId: "$balances.ClientId",
          accountId: "$balances.accountId"
        },
        total: { $sum: "$balances.amount"}
      }
    },
    {
      $project: {
        _id: 0,
        accountId: "$_id.accountId",
        date: "$_id.day",
        total: 1,
      }
    },
    {
      $sort: { day: 1}
    }
  ]
})
