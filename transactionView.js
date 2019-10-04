db.transactionView.drop()
db.runCommand(
{
  create: "transactionView",
  viewOn: "transaction",
  pipeline: [
    { $match: 
      { 
        $expr: {
          $gte:
          [
            "$bookingDate",
            {       
              $let: 
              {
                vars: { dt: { $subtract: [new Date(), 1209600000]}},
                  in: "$$dt"
              }
            }
          ]
        }
      }
    },
    {
      $project: {
        clientId: 1,
        accountId: 1,
        amount: 1,
        bookingDate: 1
      }
    }
  ]
})
