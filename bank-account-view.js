db.bankAccountView.drop()
db.runCommand(
{
    create: "bankAccountView",
    viewOn: "client",
    pipeline: [
    { $unwind: "$accountList"},
    {
        $group: {
            _id: {
                clientId: "$clientId"
            }
        }
    },
    {
        $project: {
            _id: 0,
            clientId: "$_id.clientId"
        }
    },
    { $lookup:
            {
                from: "transactionView",
                let: { clientId: "$clientId"},
                pipeline: [
                    { $match:
                            { $expr:
                                    { $and:
                                            [
                                                { $eq: [ "$clientId", "$$clientId"] }
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
        $project: {
            _id: 0,
            clientId: "$balances.clientId",
            date: "$balances.bookingDate",
            total: "$balances.bookingDateClosingBalance"
        }
    },
    {
        $sort: { date: 1}
    }
]})
