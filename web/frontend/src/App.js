import React from "react";
import axios from "axios";
const API_BASE = "http://localhost:8080/nginx/access/"

class App extends React.Component {
    ColumnItem = ({column}) => {
        return (<th> <button onClick={() => this.getSorted(column)} type='button'>{column}</button> </th> )
    }
    Rows = ({rows}) => {
        return ( 
            <tr>
                {rows.map((row) => <this.RowItem row={row}
                />)}
            </tr>)
        }

    RowItem = ({row}) => {
        return (<th> {row} </th>)
    }

    getDefaultData() {
        axios
            .get(API_BASE)
            .then(response => {
                const data = response.data;
                this.setState({'data': data})
            })
            .catch(error => console.log(error))
    }

    constructor(props) {
        super(props);
        this.getDefaultData()
    }

    getSorted(sort_by) {
    let sort
        if (this.state[`${sort_by}`]){
            this.state[`${sort_by}`] == 'asc' ? sort = 'desc': sort = 'asc'
        }else{
            sort='asc'
        }
        this.setState({ [sort_by]: sort})
        axios.get(`${API_BASE}?sort_by=${sort_by}&sort=${sort}`)
            .then(response => {
                const data = response.data;
                this.setState({'data': data})
            }).catch(error => console.log(error))
    }

    render() {
        if (this.state){
            return (
                <>
                    <div className="table">
                        <table>
                            <thead>
                            <tr>
                                {this.state.data.columns.map((col) => <this.ColumnItem
                                    column={col}
                                />)}
                            </tr>
                            </thead>
                            <tbody>
                                {this.state.data.data.map((rows) => <this.Rows
                                    rows={rows}
                                />)}
                            </tbody>

                        </table>
                    </div>
                </>
            )
        }
        else{
            return (
                <>
                </>
            )

        }

    }
}

export default App;
