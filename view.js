db.runCommand({
    create:   "bookedBalanceView",
    viewOn:   "bookedBalance", 
    pipeline: [
        {$match: {clientId: "465224-1551"}},
        {$unwind: {path: "$saldenList"}},
        {
            $project: {
                identifier: "$saldenList.bezeichnung",
                assets: "$saldenList.details",
            }
        },
        { $unwind: {path: "$assets"} },
        {
            $project: {
                _id: 0,
                identifier: 1,
                balances: "$assets.dailyBalances",
                bank: "$assets.displayText",
            }
        },
        { $unwind: {path: "$balances"} },
        {
            $match: {
                "balances.day": { "$gte": new Date((new Date()).valueOf() - ( 98 * ( 1000 * 60 * 60 * 24 ) ) ) }
            }
        },
        {
            $project:  {
                identifier: 1,
                bank: 1,
                day: "$balances.day",
                value: "$balances.saldoInCent"
            }
        },
        {
            $group: {
                _id: {
                    identifier: "$identifier",
                    day: {
                        $let: {
                            vars: {
                                ninetyEighthDays: new Date(new Date().valueOf() - (98 * ( 1000 * 60 * 60 * 24 ))),
                                ninetySecondDays: new Date(new Date().valueOf() - (92 * ( 1000 * 60 * 60 * 24 ))),
                                eightyFifthDays: new Date(new Date().valueOf() - (85 * ( 1000 * 60 * 60 * 24 ))),
                                seventyEighthDays: new Date(new Date().valueOf() - (78 * ( 1000 * 60 * 60 * 24 ))),
                                seventyFirstDays: new Date(new Date().valueOf() - (71 * ( 1000 * 60 * 60 * 24 ))),
                                sixtyFourthDays: new Date( new Date().valueOf() - (64 * ( 1000 * 60 * 60 * 24 ))),
                                fiftySeventhDays: new Date( new Date().valueOf() - (57 * ( 1000 * 60 * 60 * 24 ))),
                                fiftyFifthDays: new Date( new Date().valueOf() - (50 * ( 1000 * 60 * 60 * 24 ))),
                                fortyThirdDays: new Date( new Date().valueOf() - (43 * ( 1000 * 60 * 60 * 24 ))),
                                thirtySixthDays: new Date( new Date().valueOf() - (36 * ( 1000 * 60 * 60 * 24 ))),
                                twentyNinthDays: new Date( new Date().valueOf() - (29 * ( 1000 * 60 * 60 * 24 ))),
                                twentySecondDays: new Date( new Date().valueOf() - (22 * ( 1000 * 60 * 60 * 24 ))),
                                fifteenDays: new Date( new Date().valueOf() - (15 * ( 1000 * 60 * 60 * 24 ))),
                                sevenDays: new Date( new Date().valueOf() - (7 * ( 1000 * 60 * 60 * 24 )))
                            },
                                in: {
                                    $cond: [
                                        {$lt: ["$day", "$$ninetyEighthDays"]},
                                        "$$ninetyEighthDays",
                                        {
                                            $cond: [
                                                {$lt: ["$day", "$$ninetySecondDays"]},
                                                "$$ninetySecondDays",
                                                {
                                                    $cond: [
                                                        {$lt: ["$day", "$$eightyFifthDays"]},
                                                        "$$eightyFifthDays",
                                                        {
                                                            $cond: [
                                                                {$lt: ["$day", "$$seventyEighthDays"]},
                                                                "$$seventyEighthDays",
                                                                {
                                                                    $cond: [
                                                                        {$lt: ["$day", "$$seventyFirstDays"]},
                                                                        "$$seventyFirstDays",
                                                                        {
                                                                            $cond: [
                                                                                {$lt: ["$day", "$$sixtyFourthDays"]},
                                                                                "$$sixtyFourthDays",
                                                                                {
                                                                                    $cond: [
                                                                                        {$lt: ["$day", "$$fiftySeventhDays"]},
                                                                                        "$$fiftySeventhDays",
                                                                                        {
                                                                                            $cond: [
                                                                                                {$lt: ["$day", "$$fiftyFifthDays"]},
                                                                                                "$$fiftyFifthDays",
                                                                                                {
                                                                                                    $cond: [
                                                                                                        {$lt: ["$day", "$$fortyThirdDays"]},
                                                                                                        "$$fortyThirdDays",
                                                                                                        {
                                                                                                            $cond: [
                                                                                                                {$lt: ["$day", "$$thirtySixthDays"]},
                                                                                                                "$$thirtySixthDays",
                                                                                                                {
                                                                                                                    $cond: [
                                                                                                                        {$lt: ["$day", "$$twentyNinthDays"]},
                                                                                                                        "$$twentyNinthDays",
                                                                                                                        {
                                                                                                                            $cond: [
                                                                                                                                {$lt: ["$day", "$$twentySecondDays"]},
                                                                                                                                "$$twentySecondDays",
                                                                                                                                {
                                                                                                                                    $cond: [
                                                                                                                                        {$lt: ["$day", "$$sevenDays"]},
                                                                                                                                        "$$sevenDays",
                                                                                                                                        new Date()
                                                                                                                                    ]
                                                                                                                                }
                                                                                                                            ]
                                                                                                                        }
                                                                                                                    ]
                                                                                                                }
                                                                                                            ]
                                                                                                        }
                                                                                                    ]
                                                                                                }
                                                                                            ]
                                                                                        }
                                                                                    ]
                                                                                }
                                                                            ]
                                                                        }
                                                                    ]
                                                                }
                                                            ]
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                        }
                    }
                },
                banks: { $addToSet: "$bank"},
                total: { $sum: "$value"}
            }
        },
        {
            $facet: {
                funds: [
                    { $match: {$and: [{"_id.identifier": "summary"}]}},
                    {
                        $project: {
                            _id: 0,
                            banks: 1,
                            day: "$_id.day",
                            total: 1
                        }
                    },
                    {
                        $sort: { day: 1 }
                    }
                ],
                fundsAndDemands: [
                    { $match: { $or: [ { "_id.identifier": "summary" }, { "_id.identifier": "demands" } ] } },
                    {
                        $group: {
                            _id: "$_id.day",
                            total: { $sum: "$total"}
                        }
                    },
                    {
                        $project: {
                            _id: 0,
                            day: "$_id",
                            total: 1
                        }
                    },
                    {
                        $sort: { day: 1 }
                    }
                ],
                fundsDemandsAndDebts: [
                    { $match: { $or: [ { "_id.identifier": "summary" }, { "_id.identifier": "demands" }, { "_id.identifier": "debts" } ] } },
                    {
                        $group: {
                            _id: "$_id.day",
                            total: {
                                $sum: { 
                                    $cond: [
                                        { $eq: [ "$_id.identifier", "debts" ] },
                                        { "$subtract": [ 0, "$total" ] },
                                        "$total"
                                    ]
                                }
                            }
                        }
                    },
                    {
                        $project: {
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
});
