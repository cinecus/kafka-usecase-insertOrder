const { Sequelize, DataTypes } = require('sequelize')

// use sequenlize
const sequelize = new Sequelize({
  dialect: 'mssql',
  host: 'localhost',
  port: '1433',
  username: 'sa',
  password: 'Admin1234',
  database: 'tutorial',
  dialectOptions: {
    options: {
      encrypt: true, // Use this option if connecting to Azure SQL Database
    },
  },
});

const Order = sequelize.define('orders', {
  userLineUid: {
    type: DataTypes.STRING,
    allowNull: false
  },
  orderId: {
    type: DataTypes.STRING,
    allowNull: false
  },
  status: {
    type: DataTypes.STRING,
    allowNull: false
  }
})

const Product = sequelize.define('products', {
  name: {
    type: DataTypes.STRING,
    allowNull: false
  },
  amount: {
    type: DataTypes.INTEGER,
    allowNull: false
  }
})

Product.hasMany(Order)
Order.belongsTo(Product)

module.exports = {
  Order,
  Product,
  sequelize
}