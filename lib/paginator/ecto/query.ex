defmodule Paginator.Ecto.Query do
  @moduledoc false

  import Ecto.Query

  alias Paginator.Config

  def paginate(queryable, config \\ [])

  def paginate(queryable, %Config{} = config) do
    queryable
    |> maybe_where(config)
    |> limit(^query_limit(config))
  end

  def paginate(queryable, opts) do
    paginate(queryable, Config.new(opts))
  end

  defp filter_values(query, cursor_fields, values, operator) do
    sorts =
      cursor_fields
      |> Enum.zip(values)
      |> Enum.reject(fn val -> match?({_column, nil}, val) end)

    dynamic_sorts =
      sorts
      |> Enum.with_index()
      # Two anonymous fns, the first needs a binding name in the cursor fields
      # it uses this name to get the column for the named binding
      # the other fn only needs the name of the column and uses the first binding
      |> Enum.reduce(true, fn
        {{{binding_name, column}, value}, i}, dynamic_sorts ->
          position = Map.get(query.aliases, binding_name)

          dynamic = true

          dynamic =
            case operator do
              :lt ->
                dynamic([{q, position}], field(q, ^column) < ^value and ^dynamic)

              :gt ->
                dynamic([{q, position}], field(q, ^column) > ^value and ^dynamic)
            end

          dynamic =
            sorts
            |> Enum.take(i)
            |> Enum.reduce(dynamic, fn
              {{binding_name, prev_column}, prev_value}, dynamic ->
                p = Map.get(query.aliases, binding_name)
                dynamic([{q, p}], field(q, ^prev_column) == ^prev_value and ^dynamic)

              {prev_column, prev_value}, dynamic ->
                dynamic([{q, position}], field(q, ^prev_column) == ^prev_value and ^dynamic)
            end)

          if i == 0 do
            dynamic([{q, position}], ^dynamic and ^dynamic_sorts)
          else
            dynamic([{q, position}], ^dynamic or ^dynamic_sorts)
          end

        {{column, value}, i}, dynamic_sorts ->
          dynamic = true

          dynamic =
            case operator do
              :lt ->
                dynamic([{q, 0}], field(q, ^column) < ^value and ^dynamic)

              :gt ->
                dynamic([{q, 0}], field(q, ^column) > ^value and ^dynamic)
            end

          dynamic =
            sorts
            |> Enum.take(i)
            |> Enum.reduce(dynamic, fn {prev_column, prev_value}, dynamic ->
              dynamic([{q, 0}], field(q, ^prev_column) == ^prev_value and ^dynamic)
            end)

          if i == 0 do
            dynamic([{q, 0}], ^dynamic and ^dynamic_sorts)
          else
            dynamic([{q, 0}], ^dynamic or ^dynamic_sorts)
          end
      end)

    where(query, [{q, 0}], ^dynamic_sorts)
  end

  defp maybe_where(query, %Config{
         after_values: nil,
         before_values: nil,
         sort_direction: :asc
       }) do
    query
  end

  defp maybe_where(query, %Config{
         after_values: after_values,
         before: nil,
         cursor_fields: cursor_fields,
         sort_direction: :asc
       }) do
    query
    |> filter_values(cursor_fields, after_values, :gt)
  end

  defp maybe_where(query, %Config{
         after_values: nil,
         before_values: before_values,
         cursor_fields: cursor_fields,
         sort_direction: :asc
       }) do
    query
    |> filter_values(cursor_fields, before_values, :lt)
    |> reverse_order_bys()
  end

  defp maybe_where(query, %Config{
         after_values: after_values,
         before_values: before_values,
         cursor_fields: cursor_fields,
         sort_direction: :asc
       }) do
    query
    |> filter_values(cursor_fields, after_values, :gt)
    |> filter_values(cursor_fields, before_values, :lt)
  end

  defp maybe_where(query, %Config{
         after: nil,
         before: nil,
         sort_direction: :desc
       }) do
    query
  end

  defp maybe_where(query, %Config{
         after_values: after_values,
         before: nil,
         cursor_fields: cursor_fields,
         sort_direction: :desc
       }) do
    query
    |> filter_values(cursor_fields, after_values, :lt)
  end

  defp maybe_where(query, %Config{
         after: nil,
         before_values: before_values,
         cursor_fields: cursor_fields,
         sort_direction: :desc
       }) do
    query
    |> filter_values(cursor_fields, before_values, :gt)
    |> reverse_order_bys()
  end

  defp maybe_where(query, %Config{
         after_values: after_values,
         before_values: before_values,
         cursor_fields: cursor_fields,
         sort_direction: :desc
       }) do
    query
    |> filter_values(cursor_fields, after_values, :lt)
    |> filter_values(cursor_fields, before_values, :gt)
  end

  #  In order to return the correct pagination cursors, we need to fetch one more
  # # record than we actually want to return.
  defp query_limit(%Config{limit: limit}) do
    limit + 1
  end

  # This code was taken from https://github.com/elixir-ecto/ecto/blob/v2.1.4/lib/ecto/query.ex#L1212-L1226
  defp reverse_order_bys(query) do
    update_in(query.order_bys, fn
      [] ->
        []

      order_bys ->
        for %{expr: expr} = order_by <- order_bys do
          %{
            order_by
            | expr:
                Enum.map(expr, fn
                  {:desc, ast} -> {:asc, ast}
                  {:asc, ast} -> {:desc, ast}
                end)
          }
        end
    end)
  end
end
