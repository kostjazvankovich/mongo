db.bankAccountView.drop()
db.runCommand({
  create: "bankAccountView",
  viewOn: "bankAccount",
  pipeline: [
    { $unwind: "$accountList"},
    { $lookup: 
      {
        from: "transaction",
        let: { clientId: "$clientId", accountId: "$accountList.accountId"},
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
              _id: 0
            }
          }

        ],
        as: "balances"
      }
    },
    { 
      $project: 
      {
        _id: 0,
        bankAccountNumber: "$accountList.bankAccountNumber",
        bankCodeNumber: "$accountList.bankCodeNumber",
        bankName: "$accountList.bankName",
        currency: "$accountList.currency",
        iban: { $arrayElemAt: ["$balances.businessPartnerBankAccountIdentification", 0] },
        bic: { $arrayElemAt: ["$balances.businessPartnerBankIdentification", 0] },
        "balances.bookingDate": 1,
        "balances.amount": 1
      }
    },
    {
      $sort: { "balances.bookingDate": 1}
    }
  ]
})
